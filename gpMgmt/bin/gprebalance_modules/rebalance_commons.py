#!/usr/bin/env python3

import base64
from dataclasses import dataclass
import ipaddress
import os
import pickle
import re
import socket
from typing import Any, Dict, List, Set, Optional, Tuple
from enum import IntEnum
from gppylib.gparray import Segment, GpArray
from gppylib.commands.base import REMOTE, WorkerPool
from gppylib.commands.unix import Hostname, DiskFree, DiskUsage
from gppylib.operations.validate_disk_space import FileSystem

class ValidationError(Exception):
    pass

class ResourceError(Exception):
    pass

DEFAULT_PRIMARY_TEMPLATE = '/data1/primary/gpseg{content}'
DEFAULT_MIRROR_TEMPLATE = '/data1/mirror/gpseg{content}'

class HostStatus(IntEnum):
    ACTIVE = 1
    NEW = 2
    DECOMMISSIONED = 3

@dataclass
class DatadirInfo:
    """
    Stores both template and actual datadirs for a host
    """
    primary_template: str
    mirror_template: str
    # Actual paths from existing segment
    existing_primary_datadirs: Set[str]
    existing_mirror_datadirs: Set[str]
    
    def __init__(self, primary_template: str, mirror_template: str):
        self.primary_template = primary_template
        self.mirror_template = mirror_template
        self.existing_primary_datadirs = set()
        self.existing_mirror_datadirs = set()

@dataclass
class Host:
    """
    Segment host representaion
    
    Attributes:
        hostname: hostname from gp_segment_configuration
        address: address from gp_segment_configuration
        primary_datadirs: set of datadirs which primary catalogs belong to
        mirror_datadirs: set of datadirs which mirror catalogs belong to
        status: intendend host usage
    """
    hostname: str
    address: str
    datadir_info: DatadirInfo = None
    status: HostStatus = None

    def __hash__(self):
        return hash((self.hostname, self.address))
    
    def __eq__(self, other):
        if not isinstance(other, Host):
            return NotImplemented
        return self.hostname == other.hostname and self.address == other.address

    def __str__(self):
        pass

DISK_SPACE_SAFETY_MARGIN = 0.10

@dataclass
class SegmentSize:
    """
    Storage size of segment instance with all tablespace info
    
    Attributes:
        datadir_size_kb: Size of main datadir in KB
        tablespace_usage: Dict mapping tablespace paths to their sizes in KB
        total_size_kb: Total size including tablespaces
    """
    datadir_size_kb: int
    tablespace_usage: Optional[Dict[str, int]] = None

    @property
    def total_size_kb(self) -> int:
        """Calculate total size including tablespaces"""
        total = self.datadir_size_kb
        if self.tablespace_usage:
            total += sum(self.tablespace_usage.values())
        return total
    
    def __str__(self):
        """Human-readable size string"""
        size_mb = self.total_size_kb / 1024
        if size_mb < 1024:
            return f"{size_mb:.2f} MB"
        else:
            size_gb = size_mb / 1024
            return f"{size_gb:.2f} GB"
    
    def __repr__(self):
        return f"SegmentSize(datadir={self.datadir_size_kb}KB, tablespaces={self.tablespace_usage})"

class TemplateParser:
    """
    Handles parsing and validation of directory templates
    """
    
    VALID_PLACEHOLDERS = {'hostname', 'content'}
    PLACEHOLDER_PATTERN = r'\{(\w+)\}'
    
    @classmethod
    def parse_datadirs_input(cls, input_str: str) -> Tuple[str, str]:
        """
        Parse --target-datadirs input and return (primary_template, mirror_template)
        
        Handles:
        - "/data/primary/gpseg{content}, /data/mirror/gpseg{content}" -> as is
        - "/data/primary, /data/mirror" -> adds gpseg{content}
        - "/data/primary/{hostname}, /data/mirror/{hostname}" -> adds gpseg{content}
        - '"/data/primary", "/data/mirror"' -> removes quotes
        - --target-datadirs="/dir1, /dir2" -> handles shell quoting
        """
        # Split by comma, respecting quotes
        parts = cls._split_respecting_quotes(input_str)
        
        if len(parts) != 2:
            raise ValidationError(
                '--target-datadirs should have format: '
                '"/data/primary/gpseg{content}, /data/mirror/gpseg{content}". '
                'Available templated parameters: {hostname}, {content}'
            )
        
        # Clean and normalize each part
        primary_template = cls._normalize_template(parts[0])
        mirror_template = cls._normalize_template(parts[1])
        
        # Validate templates
        cls._validate_templates(primary_template, mirror_template)
        
        return primary_template, mirror_template
    
    @classmethod
    def _split_respecting_quotes(cls, input_str: str) -> list:
        """
        Split input string by comma, respect quoted sections.

        Handles cases like:
        - "/dir1, /dir2" -> ["/dir1", "/dir2"]
        - '"/di,r1/", "/dir2"' -> ["/di,r1/", "/dir2"]
        """      
        # Manual parsing: split by comma while respecting quotes
        parts = []
        current_part = []
        in_double_quotes = False
        in_single_quotes = False
        i = 0
        
        while i < len(input_str):
            char = input_str[i]
            
            # Track quote state
            if char == '"' and not in_single_quotes:
                in_double_quotes = not in_double_quotes
                current_part.append(char)
            elif char == "'" and not in_double_quotes:
                in_single_quotes = not in_single_quotes
                current_part.append(char)
            elif char == ',' and not in_double_quotes and not in_single_quotes:
                # Found unquoted comma - this is our delimiter
                parts.append(''.join(current_part).strip())
                current_part = []
            else:
                current_part.append(char)
            
            i += 1
        
        # Add the last part
        if current_part:
            parts.append(''.join(current_part).strip())
        
        # Clean up: remove empty parts
        parts = [p for p in parts if p]
        
        return parts

    @classmethod
    def _normalize_template(cls, path: str) -> str:
        """
        Clean path from quotes and normalize template
        - If it contains placeholders, validate and return as-is
        - If it doesn't contain {content} placeholders, append gpseg{content}
        """

        path = path.strip()
        if len(path) >= 2:
            if (path[0] == '"' and path[-1] == '"') or (path[0] == "'" and path[-1] == "'"):
                path = path[1:-1].strip()

        if not path:
            raise ValidationError('Directory path cannot be empty')
        
        if not path.startswith('/'):
            raise ValidationError(
                f'Directory path must be absolute: {path}'
            )

        placeholders = re.findall(cls.PLACEHOLDER_PATTERN, path)
        
        # Validate placeholders
        for placeholder in placeholders:
            if placeholder not in cls.VALID_PLACEHOLDERS:
                raise ValidationError(
                    f'Invalid placeholder {{{placeholder}}}. '
                    f'Valid placeholders are: {", ".join("{" + p + "}" for p in cls.VALID_PLACEHOLDERS)}'
                )
        
        # If no placeholders, add default gpseg{content}
        if not placeholders or ('content' not in placeholders):
            # Remove trailing slash if present
            path = path.rstrip('/')
            return f'{path}/gpseg{{content}}'
        
        return path
    
    @classmethod
    def _validate_templates(cls, primary_template: str, mirror_template: str) -> None:
        """
        Validate primary and mirror templates for common issues
        """
        # Check if templates are identical
        if primary_template == mirror_template:
            raise ValidationError(
                'Primary and mirror templates cannot be identical. '
                f'Both are: {primary_template}'
            )
        
        # Validate both contain {content} placeholder
        # (should always be true after normalization, but double-check)
        if '{content}' not in primary_template:
            raise ValidationError(
                f'Primary template must contain {{content}} placeholder: {primary_template}'
            )
        
        if '{content}' not in mirror_template:
            raise ValidationError(
                f'Mirror template must contain {{content}} placeholder: {mirror_template}'
            )
    
    @classmethod
    def parse_datadirs_file(cls, filepath: str) -> Tuple[str, str]:
        """
        Parse --target-datadirs-file
        Expected format (2 lines):
        /data/primary/gpseg{content}
        /data/mirror/gpseg{content}
        Available templated parameters: {hostname}, {content}
        """
        
        with open(filepath, 'r') as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
        
        if len(lines) != 2:
            raise ValidationError(
                f'File {filepath} should contain exactly 2 lines: '
                'primary template and mirror template'
            )
        
        primary_template = cls._normalize_template(lines[0])
        mirror_template = cls._normalize_template(lines[1])

        cls._validate_templates(primary_template, mirror_template)
        
        return primary_template, mirror_template
    
    @staticmethod
    def extract_parent_directory(datadir: str) -> str:
        """
        Extract parent directory from an actual segment datadir
        
        This handles arbitrary naming conventions by just getting the parent.
        
        Examples:
            /data/primary/gpseg0 -> /data/primary
        
        Returns:
            Parent directory path (without trailing slash)
        """
        return os.path.dirname(datadir.rstrip('/'))
    
    @staticmethod
    def instantiate_template(template: str, hostname: str = None, content: int = None) -> str:
        """
        Instantiate a template with actual values
        """
        result = template
        if hostname is not None:
            result = result.replace('{hostname}', hostname)
        if content is not None:
            result = result.replace('{content}', str(content))
        return result

def is_ip_address(ip_str: str):
    try:
        ipaddress.ip_address(ip_str)
        return True
    except ValueError:
        return False

class HostResolver:
    """
    Utility class to resolve and match hostnames with IP addresses
    Supports multiple IPs per hostname and maintains bidirectional mappings
    """
    def __init__(self):
        # hostname -> set of IP addresses
        self._hostname_to_ips: Dict[str, Set[str]] = {}
        # IP address -> hostname
        self._ip_to_hostname: Dict[str, str] = {}
    
    def get_address(self, hostname: str) -> str:
        """
        Get primary IP address for hostname
        Returns first IP address or hostname if not resolved
        """
        ips = self._hostname_to_ips.get(hostname, set())
        if ips:
            # Return first IP (sorted for consistency)
            return sorted(ips)[0]
        return hostname
    
    def get_all_addresses(self, hostname: str) -> Set[str]:
        """
        Get all IP addresses associated with hostname
        """
        return self._hostname_to_ips.get(hostname, set())
    
    def get_hostname(self, ip: str) -> str:
        """
        Get hostname for IP address
        Returns hostname or IP if not resolved
        """
        return self._ip_to_hostname.get(ip, ip)
    
    def resolve_hostname(self, hostname: str) -> Optional[str]:
        """
        Resolve hostname to IP addresses
        Caches all IP addresses associated with hostname
        
        Returns:
            Primary IP address (first one found), or None if resolution fails
        """
        # Already resolved
        if hostname in self._hostname_to_ips:
            return self.get_address(hostname)
        
        try:
            # Get all addresses for this hostname (IPv4 and IPv6)
            addr_info = socket.getaddrinfo(
                hostname, 
                None, 
                socket.AF_UNSPEC,
                socket.SOCK_STREAM
            )
            
            ips = set()
            for info in addr_info:
                ip = info[4][0]
                # For IPv6
                if ':' in ip:
                    ip = ip.split('%')[0]
                ips.add(ip)
            
            if ips:
                # Store hostname -> IPs mapping
                self._hostname_to_ips[hostname] = ips
                
                # Store reverse mappings (IP -> hostname)
                for ip in ips:
                    self._ip_to_hostname[ip] = hostname
                
                # Return primary IP
                return sorted(ips)[0]
            else:
                return None
                
        except (socket.gaierror, socket.error) as e:
            return None
    
    def resolve_ip(self, ip_str: str) -> Optional[str]:
        """
        Reverse resolve IP to hostname using remote command
        
        Returns:
            Hostname or None if resolution fails
        """
        # Already resolved
        if ip_str in self._ip_to_hostname:
            return self._ip_to_hostname[ip_str]
        
        try:
            # Validate it's a valid IP first
            ipaddress.ip_address(ip_str)
            
            # Get hostname from remote host
            cmd = Hostname('hostname', ctxt=REMOTE, remoteHost=ip_str)
            cmd.run()
            
            if not cmd.was_successful():
                return None
            
            hostname = cmd.get_hostname()
            
            if hostname:
                # Store reverse mapping
                self._ip_to_hostname[ip_str] = hostname
                
                # Also store forward mapping
                if hostname not in self._hostname_to_ips:
                    self._hostname_to_ips[hostname] = set()
                self._hostname_to_ips[hostname].add(ip_str)
                
                return hostname
            else:
                return None
                
        except (ValueError, Exception) as e:
            return None
    
    def hosts_match(self, host1: str, host2: str) -> bool:
        """
        Check if two hosts match (considering hostname/IP resolution)
        Handles cases where hosts are specified as hostname or IP

        Returns:
            True if hosts represent the same machine
        """
        # Direct match
        if host1 == host2:
            return True
        
        host1_normalized = host1.split('%')[0] if ':' in host1 else host1
        host2_normalized = host2.split('%')[0] if ':' in host2 else host2
        
        if host1_normalized == host2_normalized:
            return True

        # Check if both are IPs
        is_ip1 = is_ip_address(host1)
        is_ip2 = is_ip_address(host2)
        
        if is_ip1 and is_ip2:
            # Both are IPs - they don't match if not equal
            return False
        
        # One or both are hostnames - resolve and compare
        if is_ip1 and not is_ip2:
            # host1 is IP, host2 is hostname
            # Resolve host2 to get its IPs
            ips_of_host2 = self._hostname_to_ips.get(host2)
            if not ips_of_host2:
                # Try to resolve
                self.resolve_hostname(host2)
                ips_of_host2 = self._hostname_to_ips.get(host2)
            
            if ips_of_host2:
                return host1 in ips_of_host2
        
        if not is_ip1 and is_ip2:
            # host1 is hostname, host2 is IP
            # Resolve host1 to get its IPs
            ips_of_host1 = self._hostname_to_ips.get(host1)
            if not ips_of_host1:
                # Try to resolve
                self.resolve_hostname(host1)
                ips_of_host1 = self._hostname_to_ips.get(host1)
            
            if ips_of_host1:
                return host2 in ips_of_host1
        
        if not is_ip1 and not is_ip2:
            # Both are hostnames
            # Resolve both and check if they share any IPs
            ips1 = self._hostname_to_ips.get(host1)
            if not ips1:
                self.resolve_hostname(host1)
                ips1 = self._hostname_to_ips.get(host1, set())
            
            ips2 = self._hostname_to_ips.get(host2)
            if not ips2:
                self.resolve_hostname(host2)
                ips2 = self._hostname_to_ips.get(host2, set())
            
            # Check if they share any IP addresses
            if ips1 and ips2:
                return bool(ips1 & ips2)
        
        return False
    
    def find_matching_hostname(self, target_host: str, existing_hosts: List[str]) -> Optional[str]:
        """
        Find if target_host matches any existing host
        
        Returns:
            The matching existing host name, or None if no match
        """
        for existing_host in existing_hosts:
            if self.hosts_match(target_host, existing_host):
                return existing_host
        return None

def validate_hostname(hostname:str):
    if len(hostname) > 255:
        raise ValidationError(f"Hostname '{hostname}' exceeds maximum length of 255 characters")
    
    if not re.match(r'^[a-zA-Z0-9._-]+$', hostname):
        raise ValidationError(f"Hostname '{hostname}' contains invalid characters. "
        "Only ASCII letters, digits, hyphen, underscore, and dot are allowed")

def validate_hosts_basic(hosts: str, option_name: str):

    if not hosts:
        return

    target_hosts = list(map(str.strip, hosts.split(',')))

    # Remove empty strings
    target_hosts = [h for h in target_hosts if h]
    if not target_hosts:
        raise ValidationError(f" --{option_name}: No valid hosts provided")

    seen_hosts = set()
    has_ip = False
    has_hostname = False
    for host in target_hosts:
        # Check for duplicates
        if host in seen_hosts:
            raise ValidationError(f" --{option_name}: Duplicate host '{host}' found")
        seen_hosts.add(host)
        
        if is_ip_address(host):
            has_ip = True
            continue
        has_hostname = True
        validate_hostname(host)
    if has_ip and has_hostname:
        raise ValidationError(f" --{option_name} must not contain IP adress and hostname simultaniously")

def get_hosts_from_file(file, option_name) -> str:
    hosts = []
    with open(file, 'r') as fp:
        i = 0
        for line in fp:
            if i >= 1000:
                raise ValidationError(f" --{option_name} contains more than 1000 hosts")
            hostname = line.strip()
            if hostname != '':
                hosts.append(line.strip())
                i += 1
    if len(hosts) == 0:
        raise Exception(f"Empty '{file}' file")
    return ", ".join(hosts)

@dataclass
class SegmentId:
    """Identifier for a segment"""
    dbid: int
    content: int
    
    def __hash__(self):
        return hash((self.dbid, self.content))
    
    def __eq__(self, other):
        if not isinstance(other, SegmentId):
            return NotImplemented
        return self.dbid == other.dbid and self.content == other.content

@dataclass
class DiskSpaceInfo:
    """
    Disk space information for a filesystem
    
    Attributes:
        filesystem: Filesystem name/device
        available_kb: Available disk space in KB
        directory: Directory that was checked
    """
    filesystem: str
    available_kb: int
    directory: str
    
    @property
    def available_mb(self) -> float:
        return self.available_kb / 1024
    
    @property
    def available_gb(self) -> float:
        return self.available_mb / 1024
    
    def __str__(self):
        return f"Available: {self.available_gb:.2f} GB on {self.filesystem}"

class DiskSpaceChecker:
    """
    Utility for checking disk space on local and remote hosts
    """
    
    def __init__(self, logger: Any, batch_size: int = 16):
        """
        Initialize disk space checker
        
        Args:
            logger: Logger instance
            batch_size: Number of parallel operations
        """
        self.logger = logger
        self.batch_size = batch_size
    
    def get_disk_usage(self, hostaddr: str, directories: List[str]) -> Dict[str, int]:
        """
        Get the disk usage for the given set of directories on the targeted host
        
        Args:
            hostaddr: Host address (sometimes can be hostname) to check
            directories: List of directories to check
        
        Returns:
            Dictionary mapping directories to disk usage in KB
        """
        dirs_disk_usage = {}
        
        if not directories:
            return dirs_disk_usage
        
        pool = WorkerPool(numWorkers=min(len(directories), self.batch_size))
        try:
            for directory in directories:
                cmd = DiskUsage('check segment disk space used',
                               directory, ctxt=REMOTE, remoteHostAddr=hostaddr)
                pool.addCommand(cmd)
            pool.join()
        finally:
            pool.haltWork()
            pool.joinWorkers()
        
        for cmd in pool.getCompletedItems():
            if not cmd.was_successful():
                raise Exception(f"Unable to check disk usage on segment: {cmd.get_results().stderr}")
            
            dirs_disk_usage[cmd.directory] = cmd.kbytes_used()
        
        return dirs_disk_usage
    
    def get_available_space(self, hostaddr: str, directories: List[str]) -> Dict[str, DiskSpaceInfo]:
        """
        Get available disk space information for directories on remote host
        
        Uses DiskFree command which runs calculate_disk_free.py script.
        This handles the case where directories don't exist yet by walking
        up the path until it finds an existing directory.
        
        Args:
            hostaddr: Host address to check
            directories: List of directories/paths to check
        
        Returns:
            Dictionary mapping directory to DiskSpaceInfo
        """
        if not directories:
            return {}
        
        filesystems = self._get_filesystems(hostaddr, directories)
        
        # Build result mapping
        result = {}
        for fs in filesystems:
            # Each FileSystem has a list of directories it applies to
            for directory in fs.directories:
                result[directory] = DiskSpaceInfo(
                    filesystem=fs.name,
                    available_kb=fs.disk_free,
                    directory=directory
                )
        
        return result
    
    def _get_filesystems(self, hostaddr: str, directories: List[str]) -> List[FileSystem]:
        """
        Get filesystem information for directories on target host
        
        Args:
            hostaddr: Host address
            directories: List of directories
            
        Returns:
            List of FileSystem objects

        """
        filesystems = []
        
        cmd = DiskFree(hostaddr, directories)

        cmd.run()
        
        if not cmd.was_successful():
            raise Exception(f"Failed to check disk free on target segment: {cmd.get_results().stderr}")
            
        # Decode the pickled result
        filesystems = pickle.loads(
            base64.urlsafe_b64decode(cmd.get_results().stdout))
        
        return filesystems

    def check_batch_available_space(self, 
                                    directories_by_host: Dict[str, List[str]]) -> Dict[str, Dict[str, DiskSpaceInfo]]:
        """
        Check available space for multiple directories across multiple hosts
        
        Args:
            directories_by_host: Dict mapping host address to list of directories
        
        Returns:
            Dict mapping host address to dict of (directory -> DiskSpaceInfo)
        """
        results = {}
        
        for hostaddr, directories in directories_by_host.items():
            try:
                space_info = self.get_available_space(hostaddr, directories)
                results[hostaddr] = space_info
            except Exception as e:
                self.logger.error(f"Failed to check available space on {hostaddr}: {e}")
                raise
        
        return results
