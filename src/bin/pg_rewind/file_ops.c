/*-------------------------------------------------------------------------
 *
 * file_ops.c
 *	  Helper functions for operating on files.
 *
 * Most of the functions in this file are helper functions for writing to
 * the target data directory. The functions check the --dry-run flag, and
 * do nothing if it's enabled. You should avoid accessing the target files
 * directly but if you do, make sure you honor the --dry-run mode!
 *
 * Portions Copyright (c) 2013-2020, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "common/file_perm.h"
#include "common/file_utils.h"
#include "file_ops.h"
#include "filemap.h"
#include "pg_rewind.h"

/*
 * Currently open target file.
 */
static int	dstfd = -1;
static char dstpath[MAXPGPATH] = "";

static void create_target_dir(const char *path);
static void remove_target_dir(const char *path);
static void create_target_symlink(const char *path, const char *link);
static void remove_target_symlink(const char *path);
static void create_target_tablespace_layout(const char *path, const char *link);
/*
 * Open a target file for writing. If 'trunc' is true and the file already
 * exists, it will be truncated.
 */
void
open_target_file(const char *path, bool trunc)
{
	int			mode;

	if (dry_run)
		return;

	if (dstfd != -1 && !trunc &&
		strcmp(path, &dstpath[strlen(datadir_target) + 1]) == 0)
		return;					/* already open */

	close_target_file();

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);

	mode = O_WRONLY | O_CREAT | PG_BINARY;
	if (trunc)
		mode |= O_TRUNC;
	dstfd = open(dstpath, mode, pg_file_create_mode);
	if (dstfd < 0)
		pg_fatal("could not open target file \"%s\": %m",
				 dstpath);
}

/*
 * Close target file, if it's open.
 */
void
close_target_file(void)
{
	if (dstfd == -1)
		return;

	if (close(dstfd) != 0)
		pg_fatal("could not close target file \"%s\": %m",
				 dstpath);

	dstfd = -1;
}

void
write_target_range(char *buf, off_t begin, size_t size)
{
	int			writeleft;
	char	   *p;

	/* update progress report */
	fetch_done += size;
	progress_report(false);

	if (dry_run)
		return;

	if (lseek(dstfd, begin, SEEK_SET) == -1)
		pg_fatal("could not seek in target file \"%s\": %m",
				 dstpath);

	writeleft = size;
	p = buf;
	while (writeleft > 0)
	{
		int			writelen;

		errno = 0;
		writelen = write(dstfd, p, writeleft);
		if (writelen < 0)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			pg_fatal("could not write file \"%s\": %m",
					 dstpath);
		}

		p += writelen;
		writeleft -= writelen;
	}

	/* keep the file open, in case we need to copy more blocks in it */
}


void
remove_target(file_entry_t *entry)
{
	Assert(entry->action == FILE_ACTION_REMOVE);
	Assert(entry->target_exists);

	switch (entry->target_type)
	{
		case FILE_TYPE_DIRECTORY:
			remove_target_dir(entry->path);
			break;

		case FILE_TYPE_REGULAR:
			remove_target_file(entry->path, false);
			break;

		case FILE_TYPE_FIFO:
			remove_target_file(entry->path, false);
			break;

		case FILE_TYPE_SYMLINK:
			remove_target_symlink(entry->path);
			break;

		case FILE_TYPE_UNDEFINED:
			pg_fatal("undefined file type for \"%s\"", entry->path);
			break;
	}
}

void
create_target(file_entry_t *entry)
{
	Assert(entry->action == FILE_ACTION_CREATE);
	Assert(!entry->target_exists);

	switch (entry->source_type)
	{
		case FILE_TYPE_DIRECTORY:
			create_target_dir(entry->path);
			break;

		case FILE_TYPE_SYMLINK:
			if (entry->is_gp_tablespace)
				create_target_tablespace_layout(entry->path, entry->source_link_target);
			else
				create_target_symlink(entry->path, entry->source_link_target);
			break;

		case FILE_TYPE_REGULAR:
			/* can't happen. Regular files are created with open_target_file. */
			pg_fatal("invalid action (CREATE) for regular file");
			break;

		case FILE_TYPE_FIFO:
			/* Only pgsql_tmp files are FIFO and they are ignored from source target. */
			pg_fatal("invalid action (CREATE) for fifo file");
			break;

		case FILE_TYPE_UNDEFINED:
			pg_fatal("undefined file type for \"%s\"", entry->path);
			break;
	}
}

/*
 * Remove a file from target data directory.  If missing_ok is true, it
 * is fine for the target file to not exist.
 */
void
remove_target_file(const char *path, bool missing_ok)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (unlink(dstpath) != 0)
	{
		if (errno == ENOENT && missing_ok)
			return;

		pg_fatal("could not remove file \"%s\": %m",
				 dstpath);
	}
}

void
truncate_target_file(const char *path, off_t newsize)
{
	char		dstpath[MAXPGPATH];
	int			fd;

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);

	fd = open(dstpath, O_WRONLY, pg_file_create_mode);
	if (fd < 0)
		pg_fatal("could not open file \"%s\" for truncation: %m",
				 dstpath);

	if (ftruncate(fd, newsize) != 0)
		pg_fatal("could not truncate file \"%s\" to %u: %m",
				 dstpath, (unsigned int) newsize);

	close(fd);
}

static void
create_target_dir(const char *path)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (mkdir(dstpath, pg_dir_create_mode) != 0)
		pg_fatal("could not create directory \"%s\": %m",
				 dstpath);
}

static void
remove_target_dir(const char *path)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (rmdir(dstpath) != 0)
		pg_fatal("could not remove directory \"%s\": %m",
				 dstpath);
}

static void
create_target_symlink(const char *path, const char *link)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (symlink(link, dstpath) != 0)
		pg_fatal("could not create symbolic link at \"%s\": %m",
				 dstpath);
}

static void
remove_target_symlink(const char *path)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (unlink(dstpath) != 0)
		pg_fatal("could not remove symbolic link \"%s\": %m",
				 dstpath);
}

/* Create symlink for tablespace, create tablespace target dir */
static void
create_target_tablespace_layout(const char *path, const char *link)
{
	char		dstpath[MAXPGPATH];
	char		*newlink;

	if (dry_run)
		return;

	/* Append the target dbid to the symlink target. */
	newlink = psprintf("%s/%d", link, dbid_target);

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (symlink(newlink, dstpath) != 0)
		pg_fatal("could not create symbolic link at \"%s\": %m",
				 dstpath);

	/* We need to create the directory at the symlink target. */
	if (mkdir(newlink, S_IRWXU) != 0)
		pg_fatal("could not create directory \"%s\": %m",
				 newlink);

	pfree(newlink);
}

/*
 * Sync target data directory to ensure that modifications are safely on disk.
 *
 * We do this once, for the whole data directory, for performance reasons.  At
 * the end of pg_rewind's run, the kernel is likely to already have flushed
 * most dirty buffers to disk.  Additionally fsync_pgdata uses a two-pass
 * approach (only initiating writeback in the first pass), which often reduces
 * the overall amount of IO noticeably.
 *
 * gpdb: We assume that all files are synchronized before rewinding and thus we
 * just need to synchronize those affected files. This is a resonable
 * assumption for gpdb since we've ensured that the db state is clean shutdown
 * in pg_rewind by running single mode postgres if needed and also we do not
 * copy an unsynchronized dababase without sync as the target base.
 */
void
sync_target_dir(filemap_t *filemap)
{
	if (!do_sync || dry_run)
		return;

	file_entry_t *entry;
	int			  i;

	if (chdir(datadir_target) < 0)
	{
		pg_log_error("could not change directory to \"%s\": %m", datadir_target);
		exit(1);
	}

	for (i = 0; i < filemap->nentries; i++)
	{
		entry = filemap->entries[i];

		if (entry->target_pages_to_overwrite.bitmapsize > 0)
			fsync_fname(entry->path, false);
		else
		{
			switch (entry->action)
			{
				case FILE_ACTION_COPY:
				case FILE_ACTION_TRUNCATE:
				case FILE_ACTION_COPY_TAIL:
					fsync_fname(entry->path, false);
					break;

				case FILE_ACTION_CREATE:
					fsync_fname(entry->path,
								entry->source_type == FILE_TYPE_DIRECTORY);
					/* FALLTHROUGH */
				case FILE_ACTION_REMOVE:
					/*
					 * Fsync the parent directory if we either create or delete
					 * files/directories in the parent directory. The parent
					 * directory might be missing as expected, so fsync it could
					 * fail but we ignore that error.
					 */
					fsync_parent_path(entry->path);
					break;

				case FILE_ACTION_NONE:
					break;

				default:
					pg_fatal("no action decided for \"%s\"", entry->path);
					break;
			}
		}
	}

	/* fsync some files that are (possibly) written by pg_rewind. */
	fsync_fname("global/pg_control", false);
	fsync_fname("backup_label", false);
	fsync_fname("postgresql.auto.conf", false);
	fsync_fname(".", true); /* due to new file backup_label. */
}

/*
 * Read a file into memory. The file to be read is <datadir>/<path>.
 * The file contents are returned in a malloc'd buffer, and *filesize
 * is set to the length of the file.
 *
 * The returned buffer is always zero-terminated; the size of the returned
 * buffer is actually *filesize + 1. That's handy when reading a text file.
 * This function can be used to read binary files as well, you can just
 * ignore the zero-terminator in that case.
 *
 * This function is used to implement the fetchFile function in the "fetch"
 * interface (see fetch.c), but is also called directly.
 */
char *
slurpFile(const char *datadir, const char *path, size_t *filesize)
{
	int			fd;
	char	   *buffer;
	struct stat statbuf;
	char		fullpath[MAXPGPATH];
	int			len;
	int			r;

	snprintf(fullpath, sizeof(fullpath), "%s/%s", datadir, path);

	if ((fd = open(fullpath, O_RDONLY | PG_BINARY, 0)) == -1)
		pg_fatal("could not open file \"%s\" for reading: %m",
				 fullpath);

	if (fstat(fd, &statbuf) < 0)
		pg_fatal("could not open file \"%s\" for reading: %m",
				 fullpath);

	len = statbuf.st_size;

	buffer = pg_malloc(len + 1);

	r = read(fd, buffer, len);
	if (r != len)
	{
		if (r < 0)
			pg_fatal("could not read file \"%s\": %m",
					 fullpath);
		else
			pg_fatal("could not read file \"%s\": read %d of %zu",
					 fullpath, r, (Size) len);
	}
	close(fd);

	/* Zero-terminate the buffer. */
	buffer[len] = '\0';

	if (filesize)
		*filesize = len;
	return buffer;
}
