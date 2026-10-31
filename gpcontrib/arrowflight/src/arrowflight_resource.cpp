/*-------------------------------------------------------------------------
 *
 * arrowflight_resource.cpp
 *	  ResourceOwner integration for Arrow Flight client state.
 *
 *-------------------------------------------------------------------------
 */

#include "arrowflight_internal.h"

#include <new>
#include <stdexcept>

typedef struct ArrowFlightResource
{
	void	   *resource;
	void	  **resource_slot;
	ResourceOwner owner;
	ArrowFlightResourceCleanup cleanup;
	const char *description;
	struct ArrowFlightResource *previous;
	struct ArrowFlightResource *next;
} ArrowFlightResource;

static ArrowFlightResource *open_resources = NULL;
static bool resource_callback_registered = false;

static void af_resource_release_callback(ResourceReleasePhase phase,
										 bool is_commit,
										 bool is_top_level,
										 void *arg);
static void af_resource_unlink(ArrowFlightResource *resource);

void
af_resource_register(void *resource, ArrowFlightResourceCleanup cleanup,
					 const char *description)
{
	ArrowFlightResource *entry;

	if (resource == NULL || cleanup == NULL)
		throw std::runtime_error(
			"cannot register an empty Arrow Flight resource");
	if (CurrentResourceOwner == NULL)
		throw std::runtime_error(
			"cannot register an Arrow Flight resource without a resource owner");

	for (entry = open_resources; entry != NULL; entry = entry->next)
	{
		if (entry->resource == resource)
			throw std::runtime_error(
				"Arrow Flight resource is already registered");
	}

	entry = new (std::nothrow) ArrowFlightResource{};
	if (entry == NULL)
		throw std::bad_alloc();

	entry->resource = resource;
	entry->owner = CurrentResourceOwner;
	entry->cleanup = cleanup;
	entry->description = description;
	entry->next = open_resources;
	if (open_resources != NULL)
		open_resources->previous = entry;
	open_resources = entry;

	if (!resource_callback_registered)
	{
		RegisterResourceReleaseCallback(af_resource_release_callback, NULL);
		resource_callback_registered = true;
	}
}

void
af_resource_attach(void **resource_slot)
{
	ArrowFlightResource *entry;

	if (resource_slot == NULL || *resource_slot == NULL)
		elog(ERROR, "cannot attach an empty Arrow Flight resource slot");

	for (entry = open_resources; entry != NULL; entry = entry->next)
	{
		if (entry->resource == *resource_slot)
		{
			if (entry->resource_slot != NULL &&
				entry->resource_slot != resource_slot)
				elog(ERROR, "Arrow Flight resource is already attached");
			entry->resource_slot = resource_slot;
			return;
		}
	}

	elog(ERROR, "Arrow Flight resource is not registered");
}

void
af_resource_unregister(void *resource)
{
	ArrowFlightResource *entry;

	if (resource == NULL)
		return;

	for (entry = open_resources; entry != NULL; entry = entry->next)
	{
		if (entry->resource == resource)
		{
			af_resource_unlink(entry);
			delete entry;
			return;
		}
	}
}

static void
af_resource_release_callback(ResourceReleasePhase phase, bool is_commit,
							 bool is_top_level, void *arg)
{
	ArrowFlightResource *entry;
	ArrowFlightResource *next;

	(void) is_top_level;
	(void) arg;

	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;

	for (entry = open_resources; entry != NULL; entry = next)
	{
		next = entry->next;
		if (entry->owner != CurrentResourceOwner)
			continue;

		af_resource_unlink(entry);
		if (entry->resource_slot != NULL &&
			*entry->resource_slot == entry->resource)
			*entry->resource_slot = NULL;
		if (is_commit)
			elog(WARNING, "%s resource leak: %p is still open",
				 entry->description, entry->resource);
		entry->cleanup(entry->resource);
		delete entry;
	}
}

static void
af_resource_unlink(ArrowFlightResource *resource)
{
	if (resource->previous != NULL)
		resource->previous->next = resource->next;
	else
		open_resources = resource->next;

	if (resource->next != NULL)
		resource->next->previous = resource->previous;

	resource->previous = NULL;
	resource->next = NULL;
}
