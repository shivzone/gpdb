/*-------------------------------------------------------------------------
 *
 * catalog.c
 *		routines concerned with catalog naming conventions and other
 *		bits of hard-wired knowledge
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/catalog.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/genam.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_auth_time_constraint.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_pltemplate.h"
#include "catalog/pg_resourcetype.h"
#include "catalog/pg_resqueue.h"
#include "catalog/pg_resqueuecapability.h"
#include "catalog/pg_resgroup.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_shseclabel.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_trigger.h"

#include "catalog/gp_configuration_history.h"
#include "catalog/gp_segment_config.h"
#include "catalog/pg_stat_last_operation.h"
#include "catalog/pg_stat_last_shoperation.h"

#include "catalog/gp_id.h"
#include "catalog/gp_version.h"
#include "catalog/toasting.h"

#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/tqual.h"

#include "cdb/cdbvars.h"

#define FORKNAMECHARS	4		/* max chars for a fork name */

/*
 * Lookup table of fork name by fork number.
 *
 * If you add a new entry, remember to update the errhint below, and the
 * documentation for pg_relation_size(). Also keep FORKNAMECHARS above
 * up-to-date.
 */
const char *forkNames[] = {
	"main",						/* MAIN_FORKNUM */
	"fsm",						/* FSM_FORKNUM */
	"vm",						/* VISIBILITYMAP_FORKNUM */
	"init"						/* INIT_FORKNUM */
};

/*
 * forkname_to_number - look up fork number by name
 */
ForkNumber
forkname_to_number(char *forkName)
{
	ForkNumber	forkNum;

	for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
		if (strcmp(forkNames[forkNum], forkName) == 0)
			return forkNum;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid fork name"),
			 errhint("Valid fork names are \"main\", \"fsm\", and \"vm\".")));
	return InvalidForkNumber;	/* keep compiler quiet */
}

/*
 * Return directory name within tablespace location to use, for this server.
 * This is the GPDB replacement for PostgreSQL's TABLESPACE_VERSION_DIRECTORY
 * constant.
 */
const char *
tablespace_version_directory(void)
{
	static char path[MAXPGPATH] = "";

	if (!path[0])
		snprintf(path, MAXPGPATH, "%s_db%d", GP_TABLESPACE_VERSION_DIRECTORY, GpIdentity.dbid);

	return path;
}

/*
 * forkname_chars
 *		We use this to figure out whether a filename could be a relation
 *		fork (as opposed to an oddly named stray file that somehow ended
 *		up in the database directory).	If the passed string begins with
 *		a fork name (other than the main fork name), we return its length,
 *		and set *fork (if not NULL) to the fork number.  If not, we return 0.
 *
 * Note that the present coding assumes that there are no fork names which
 * are prefixes of other fork names.
 */
int
forkname_chars(const char *str, ForkNumber *fork)
{
	ForkNumber	forkNum;

	for (forkNum = 1; forkNum <= MAX_FORKNUM; forkNum++)
	{
		int			len = strlen(forkNames[forkNum]);

		if (strncmp(forkNames[forkNum], str, len) == 0)
		{
			if (fork)
				*fork = forkNum;
			return len;
		}
	}
	return 0;
}

/*
 * relpathbackend - construct path to a relation's file
 *
 * Result is a palloc'd string.
 *
 * In PostgreSQL, the 'backendid' is embedded in the filename of temporary
 * relations. In GPDB, however, temporary relations are just prefixed with
 * "t_*", without the backend id. For compatibility with upstream code, this
 * function still takes 'backendid' as argument, but we only care whether
 * it's InvalidBackendId or not. If you need to construct the path of a
 * temporary relation, but don't know the real backend ID, pass
 * TempRelBackendId.
 */
char *
relpathbackend(RelFileNode rnode, BackendId backend, ForkNumber forknum)
{
	int			pathlen;
	char	   *path;

	if (rnode.spcNode == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		Assert(rnode.dbNode == 0);
		Assert(backend == InvalidBackendId);
		pathlen = 7 + OIDCHARS + 1 + FORKNAMECHARS + 1;
		path = (char *) palloc(pathlen);
		if (forknum != MAIN_FORKNUM)
			snprintf(path, pathlen, "global/%u_%s",
					 rnode.relNode, forkNames[forknum]);
		else
			snprintf(path, pathlen, "global/%u", rnode.relNode);
	}
	else if (rnode.spcNode == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		if (backend == InvalidBackendId)
		{
			pathlen = 5 + OIDCHARS + 1 + OIDCHARS + 1 + FORKNAMECHARS + 1;
			path = (char *) palloc(pathlen);
			if (forknum != MAIN_FORKNUM)
				snprintf(path, pathlen, "base/%u/%u_%s",
						 rnode.dbNode, rnode.relNode,
						 forkNames[forknum]);
			else
				snprintf(path, pathlen, "base/%u/%u",
						 rnode.dbNode, rnode.relNode);
		}
		else
		{
			/* OIDCHARS will suffice for an integer, too */
			pathlen = 5 + OIDCHARS + 2 + OIDCHARS + 1 + OIDCHARS + 1
				+ FORKNAMECHARS + 1;
			path = (char *) palloc(pathlen);
			if (forknum != MAIN_FORKNUM)
				snprintf(path, pathlen, "base/%u/t_%u_%s",
						 rnode.dbNode, rnode.relNode,
						 forkNames[forknum]);
			else
				snprintf(path, pathlen, "base/%u/t_%u",
						 rnode.dbNode, rnode.relNode);
		}
	}
	else
	{
		/* All other tablespaces are accessed via symlinks */
		if (backend == InvalidBackendId)
		{
			pathlen = 9 + 1 + OIDCHARS + 1
				+ strlen(tablespace_version_directory()) + 1 + OIDCHARS + 1
				+ OIDCHARS + 1 + FORKNAMECHARS + 1;
			path = (char *) palloc(pathlen);
			if (forknum != MAIN_FORKNUM)
				snprintf(path, pathlen, "pg_tblspc/%u/%s/%u/%u_%s",
						 rnode.spcNode, tablespace_version_directory(),
						 rnode.dbNode, rnode.relNode,
						 forkNames[forknum]);
			else
				snprintf(path, pathlen, "pg_tblspc/%u/%s/%u/%u",
						 rnode.spcNode, tablespace_version_directory(),
						 rnode.dbNode, rnode.relNode);
		}
		else
		{
			/* OIDCHARS will suffice for an integer, too */
			pathlen = 9 + 1 + OIDCHARS + 1
				+ strlen(tablespace_version_directory()) + 1 + OIDCHARS + 2
				+ OIDCHARS + 1 + OIDCHARS + 1 + FORKNAMECHARS + 1;
			path = (char *) palloc(pathlen);
			if (forknum != MAIN_FORKNUM)
				snprintf(path, pathlen, "pg_tblspc/%u/%s/%u/t_%u_%s",
						 rnode.spcNode, tablespace_version_directory(),
						 rnode.dbNode, rnode.relNode,
						 forkNames[forknum]);
			else
				snprintf(path, pathlen, "pg_tblspc/%u/%s/%u/t_%u",
						 rnode.spcNode, tablespace_version_directory(),
						 rnode.dbNode, rnode.relNode);
		}
	}
	return path;
}

/*
 * Like relpath(), but gets the directory containing the data file
 * and the filename separately.
 */
void
reldir_and_filename(RelFileNode node, BackendId backend, ForkNumber forknum,
					char **dir, char **filename)
{
	char	   *path;
	int			i;

	path = relpathbackend(node, backend, forknum);

	/*
	 * The base path is like "<path>/<rnode>". Split it into
	 * path and filename parts.
	 */
	for (i = strlen(path) - 1; i >= 0; i--)
	{
		if (path[i] == '/')
			break;
	}
	if (i <= 0 || path[i] != '/')
		elog(ERROR, "unexpected path: \"%s\"", path);

	*dir = pnstrdup(path, i);
	*filename = pstrdup(&path[i + 1]);

	pfree(path);
}

/*
 * Like relpathbackend(), but more convenient when dealing with
 * AO relations. The filename pattern is the same as for heap
 * tables, but this variant takes also 'segno' as argument.
 */
char *
aorelpathbackend(RelFileNode node, BackendId backend, int32 segno)
{
	char	   *fullpath;
	char	   *path;

	path = relpathbackend(node, backend, MAIN_FORKNUM);
	if (segno == 0)
		fullpath = path;
	else
	{
		/* be sure we have enough space for the '.segno' */
		fullpath = (char *) palloc(strlen(path) + 12);
		sprintf(fullpath, "%s.%u", path, segno);
		pfree(path);
	}
	return fullpath;
}

/*
 * GetDatabasePath			- construct path to a database dir
 *
 * Result is a palloc'd string.
 *
 * XXX this must agree with relpath()!
 */
char *
GetDatabasePath(Oid dbNode, Oid spcNode)
{
	int			pathlen;
	char	   *path;

	if (spcNode == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		Assert(dbNode == 0);
		pathlen = 6 + 1;
		path = (char *) palloc(pathlen);
		snprintf(path, pathlen, "global");
	}
	else if (spcNode == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		pathlen = 5 + OIDCHARS + 1;
		path = (char *) palloc(pathlen);
		snprintf(path, pathlen, "base/%u",
				 dbNode);
	}
	else
	{
		/* All other tablespaces are accessed via symlinks */
		path = psprintf("pg_tblspc/%u/%s/%u",
						spcNode, tablespace_version_directory(), dbNode);
	}
	return path;
}

/*
 * IsSystemRelation
 *		True iff the relation is a system catalog relation.
 *
 *		NB: TOAST relations are considered system relations by this test
 *		for compatibility with the old IsSystemRelationName function.
 *		This is appropriate in many places but not all.  Where it's not,
 *		also check IsToastRelation.
 *
 *		We now just test if the relation is in the system catalog namespace;
 *		so it's no longer necessary to forbid user relations from having
 *		names starting with pg_.
 */
bool
IsSystemRelation(Relation relation)
{
	return IsSystemNamespace(RelationGetNamespace(relation)) ||
		   IsToastNamespace(RelationGetNamespace(relation)) ||
		   IsAoSegmentNamespace(RelationGetNamespace(relation));
}

/*
 * IsSystemClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
bool
IsSystemClass(Form_pg_class reltuple)
{
	Oid			relnamespace = reltuple->relnamespace;

	return IsSystemNamespace(relnamespace) ||
		IsToastNamespace(relnamespace) ||
		IsAoSegmentNamespace(relnamespace);
}

/*
 * IsToastRelation
 *		True iff relation is a TOAST support relation (or index).
 */
bool
IsToastRelation(Relation relation)
{
	return IsToastNamespace(RelationGetNamespace(relation));
}

/*
 * IsToastClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
bool
IsToastClass(Form_pg_class reltuple)
{
	Oid			relnamespace = reltuple->relnamespace;

	return IsToastNamespace(relnamespace);
}

/*
 * IsSystemNamespace
 *		True iff namespace is pg_catalog.
 *
 * NOTE: the reason this isn't a macro is to avoid having to include
 * catalog/pg_namespace.h in a lot of places.
 */
bool
IsSystemNamespace(Oid namespaceId)
{
	return namespaceId == PG_CATALOG_NAMESPACE;
}

/*
 * IsToastNamespace
 *		True iff namespace is pg_toast or my temporary-toast-table namespace.
 *
 * Note: this will return false for temporary-toast-table namespaces belonging
 * to other backends.  Those are treated the same as other backends' regular
 * temp table namespaces, and access is prevented where appropriate.
 */
bool
IsToastNamespace(Oid namespaceId)
{
	return (namespaceId == PG_TOAST_NAMESPACE) ||
		isTempToastNamespace(namespaceId);
}

/*
 * IsAoSegmentNamespace
 *		True iff namespace is pg_aoseg.
 *
 * NOTE: the reason this isn't a macro is to avoid having to include
 * catalog/pg_namespace.h in a lot of places.
 */
bool
IsAoSegmentNamespace(Oid namespaceId)
{
	return namespaceId == PG_AOSEGMENT_NAMESPACE;
}

/*
 * IsReservedName
 *		True iff name starts with the pg_ prefix.
 *
 *		For some classes of objects, the prefix pg_ is reserved for
 *		system objects only.  As of 8.0, this is only true for
 *		schema and tablespace names.
 *
 *      As of Greenplum 4.0 we also reserve the prefix gp_
 */
bool
IsReservedName(const char *name)
{
	/* ugly coding for speed */
	return ((name[0] == 'p' && name[1] == 'g' && name[2] == '_') ||
			(name[0] == 'g' && name[1] == 'p' && name[2] == '_'));
}

/*
 * GetReservedPrefix
 *		Given a string that is a reserved name return the portion of
 *      the name that makes it reserved - the reserved prefix.
 *
 *      Current return values include "pg_" and "gp_"
 */
char *
GetReservedPrefix(const char *name)
{
	char		*prefix = NULL;

	if (IsReservedName(name))
	{
		prefix = palloc(4);
		memcpy(prefix, name, 3);
		prefix[3] = '\0';
	}

	return prefix;
}

/*
 * IsSharedRelation
 *		Given the OID of a relation, determine whether it's supposed to be
 *		shared across an entire database cluster.
 *
 * Hard-wiring this list is pretty grotty, but we really need it so that
 * we can compute the locktag for a relation (and then lock it) without
 * having already read its pg_class entry.	If we try to retrieve relisshared
 * from pg_class with no pre-existing lock, there is a race condition against
 * anyone who is concurrently committing a change to the pg_class entry:
 * since we read system catalog entries under SnapshotNow, it's possible
 * that both the old and new versions of the row are invalid at the instants
 * we scan them.  We fix this by insisting that updaters of a pg_class
 * row must hold exclusive lock on the corresponding rel, and that users
 * of a relation must hold at least AccessShareLock on the rel *before*
 * trying to open its relcache entry.  But to lock a rel, you have to
 * know if it's shared.  Fortunately, the set of shared relations is
 * fairly static, so a hand-maintained list of their OIDs isn't completely
 * impractical.
 */
bool
IsSharedRelation(Oid relationId)
{
	/* These are the shared catalogs (look for BKI_SHARED_RELATION) */
	if (relationId == AuthIdRelationId ||
		relationId == AuthMemRelationId ||
		relationId == DatabaseRelationId ||
		relationId == PLTemplateRelationId ||
		relationId == SharedDescriptionRelationId ||
		relationId == SharedDependRelationId ||
		relationId == SharedSecLabelRelationId ||
		relationId == TableSpaceRelationId ||
		relationId == DbRoleSettingRelationId)
		return true;

	/* GPDB additions */
	if (relationId == GpIdRelationId ||
		relationId == GpVersionRelationId ||

		/* MPP-6929: metadata tracking */
		relationId == StatLastShOpRelationId ||

		relationId == ResQueueRelationId ||
		relationId == ResourceTypeRelationId ||
		relationId == ResQueueCapabilityRelationId ||
		relationId == ResGroupRelationId ||
		relationId == ResGroupCapabilityRelationId ||
		relationId == GpConfigHistoryRelationId ||
		relationId == GpSegmentConfigRelationId ||

		relationId == AuthTimeConstraintRelationId)
		return true;

	/* These are their indexes (see indexing.h) */
	if (relationId == AuthIdRolnameIndexId ||
		relationId == AuthIdOidIndexId ||
		relationId == AuthMemRoleMemIndexId ||
		relationId == AuthMemMemRoleIndexId ||
		relationId == DatabaseNameIndexId ||
		relationId == DatabaseOidIndexId ||
		relationId == PLTemplateNameIndexId ||
		relationId == SharedDescriptionObjIndexId ||
		relationId == SharedDependDependerIndexId ||
		relationId == SharedDependReferenceIndexId ||
		relationId == SharedSecLabelObjectIndexId ||
		relationId == TablespaceOidIndexId ||
		relationId == TablespaceNameIndexId ||
		relationId == DbRoleSettingDatidRolidIndexId)
		return true;

	/* GPDB added indexes */
	if (/* MPP-6929: metadata tracking */
		relationId == StatLastShOpClassidObjidIndexId ||
		relationId == StatLastShOpClassidObjidStaactionnameIndexId ||

		relationId == ResQueueOidIndexId ||
		relationId == ResQueueRsqnameIndexId ||
		relationId == ResourceTypeOidIndexId ||
		relationId == ResourceTypeRestypidIndexId ||
		relationId == ResourceTypeResnameIndexId ||
		relationId == ResQueueCapabilityOidIndexId ||
		relationId == ResQueueCapabilityResqueueidIndexId ||
		relationId == ResQueueCapabilityRestypidIndexId ||
		relationId == ResGroupOidIndexId ||
		relationId == ResGroupRsgnameIndexId ||
		relationId == ResGroupCapabilityOidIndexId ||
		relationId == ResGroupCapabilityResgroupidIndexId ||
		relationId == ResGroupCapabilityResgroupidResLimittypeIndexId ||
		relationId == AuthIdRolResQueueIndexId ||
		relationId == AuthIdRolResGroupIndexId ||
		relationId == GpSegmentConfigContentPreferred_roleIndexId ||
		relationId == GpSegmentConfigDbidIndexId ||
		relationId == AuthTimeConstraintAuthIdIndexId)
	{
		return true;
	}

	/* These are their toast tables and toast indexes (see toasting.h) */
	if (relationId == PgShdescriptionToastTable ||
		relationId == PgShdescriptionToastIndex ||
		relationId == PgDbRoleSettingToastTable ||
		relationId == PgDbRoleSettingToastIndex)
		return true;

	/* GPDB added toast tables and their indexes */
	if (relationId == GpSegmentConfigToastTable ||
		relationId == GpSegmentConfigToastIndex)
	{
		return true;
	}
	return false;
}

/*
 * OIDs for catalog object are normally allocated in the master, and
 * executor nodes should just use the OIDs passed by the master. But
 * there are some exceptions.
 */
static bool
RelationNeedsSynchronizedOIDs(Relation relation)
{
	if (IsSystemNamespace(RelationGetNamespace(relation)))
	{
		switch(RelationGetRelid(relation))
		{
			/*
			 * pg_largeobject is more like a user table, and has
			 * different contents in each segment and master.
			 */
			case LargeObjectRelationId:
				return false;

			/*
			 * We don't currently synchronize the OIDs of these catalogs.
			 * It's a bit sketchy that we don't, but we get away with it
			 * because these OIDs don't appear in any of the Node structs
			 * that are dispatched from master to segments. (Except for the
			 * OIDs, the contents of these tables should be in sync.)
			 */
			case RewriteRelationId:
			case TriggerRelationId:
			case AccessMethodOperatorRelationId:
			case AccessMethodProcedureRelationId:
				return false;
		}

		/*
		 * All other system catalogs are assumed to need synchronized
		 * OIDs.
		 */
		return true;
	}
	return false;
}

/*
 * GetNewOid
 *		Generate a new OID that is unique within the given relation.
 *
 * Caller must have a suitable lock on the relation.
 *
 * Uniqueness is promised only if the relation has a unique index on OID.
 * This is true for all system catalogs that have OIDs, but might not be
 * true for user tables.  Note that we are effectively assuming that the
 * table has a relatively small number of entries (much less than 2^32)
 * and there aren't very long runs of consecutive existing OIDs.  Again,
 * this is reasonable for system catalogs but less so for user tables.
 *
 * Since the OID is not immediately inserted into the table, there is a
 * race condition here; but a problem could occur only if someone else
 * managed to cycle through 2^32 OIDs and generate the same OID before we
 * finish inserting our row.  This seems unlikely to be a problem.	Note
 * that if we had to *commit* the row to end the race condition, the risk
 * would be rather higher; therefore we use SnapshotDirty in the test,
 * so that we will see uncommitted rows.
 */
Oid
GetNewOid(Relation relation)
{
	Oid			newOid;
	Oid			oidIndex;

	/* If relation doesn't have OIDs at all, caller is confused */
	Assert(relation->rd_rel->relhasoids);

	/* In bootstrap mode, we don't have any indexes to use */
	if (IsBootstrapProcessingMode())
		return GetNewObjectId();

	/* The relcache will cache the identity of the OID index for us */
	oidIndex = RelationGetOidIndex(relation);

	/* If no OID index, just hand back the next OID counter value */
	if (!OidIsValid(oidIndex))
	{
		/*
		 * System catalogs that have OIDs should *always* have a unique OID
		 * index; we should only take this path for user tables. Give a
		 * warning if it looks like somebody forgot an index.
		 */
		if (IsSystemRelation(relation))
			elog(WARNING, "generating possibly-non-unique OID for \"%s\"",
				 RelationGetRelationName(relation));

		return GetNewObjectId();
	}

	/* Otherwise, use the index to find a nonconflicting OID */
	do {
		newOid = GetNewOidWithIndex(relation, oidIndex, ObjectIdAttributeNumber);
	} while(!IsOidAcceptable(newOid));

	/*
	 * Most catalog objects need to have the same OID in the master and all
	 * segments. When creating a new object, the master should allocate the
	 * OID and tell the segments to use the same, so segments should have no
	 * need to ever allocate OIDs on their own. Therefore, give a WARNING if
	 * GetNewOid() is called in a segment. (There are a few exceptions, see
	 * RelationNeedsSynchronizedOIDs()).
	 */
	if (Gp_role == GP_ROLE_EXECUTE && RelationNeedsSynchronizedOIDs(relation))
		elog(PANIC, "allocated OID %u for relation \"%s\" in segment",
			 newOid, RelationGetRelationName(relation));

	return newOid;
}

/*
 * GetNewOidWithIndex
 *		Guts of GetNewOid: use the supplied index
 *
 * This is exported separately because there are cases where we want to use
 * an index that will not be recognized by RelationGetOidIndex: TOAST tables
 * and pg_largeobject have indexes that are usable, but have multiple columns
 * and are on ordinary columns rather than a true OID column.  This code
 * will work anyway, so long as the OID is the index's first column.  The
 * caller must pass in the actual heap attnum of the OID column, however.
 *
 * Caller must have a suitable lock on the relation.
 */
Oid
GetNewOidWithIndex(Relation relation, Oid indexId, AttrNumber oidcolumn)
{
	Oid			newOid;
	SnapshotData SnapshotDirty;
	SysScanDesc scan;
	ScanKeyData key;
	bool		collides;

	InitDirtySnapshot(SnapshotDirty);

	/* Generate new OIDs until we find one not in the table */
	do
	{
		CHECK_FOR_INTERRUPTS();

		newOid = GetNewObjectId();

		ScanKeyInit(&key,
					oidcolumn,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(newOid));

		/* see notes above about using SnapshotDirty */
		scan = systable_beginscan(relation, indexId, true,
								  &SnapshotDirty, 1, &key);

		collides = HeapTupleIsValid(systable_getnext(scan));

		systable_endscan(scan);
	} while (collides);

	return newOid;
}

static bool
GpCheckRelFileCollision(RelFileNodeBackend rnode)
{
	char	   *rpath;
	bool		collides;

	/* Check for existing file of same name */
	rpath = relpath(rnode, MAIN_FORKNUM);
	if (access(rpath, F_OK) == 0)
		collides = true;
	else
	{
		/*
		 * Here we have a little bit of a dilemma: if errno is something
		 * other than ENOENT, should we declare a collision and loop? In
		 * particular one might think this advisable for, say, EPERM.
		 * However there really shouldn't be any unreadable files in a
		 * tablespace directory, and if the EPERM is actually complaining
		 * that we can't read the directory itself, we'd be in an infinite
		 * loop.  In practice it seems best to go ahead regardless of the
		 * errno.  If there is a colliding file we will get an smgr
		 * failure when we attempt to create the new relation file.
		 */
		collides = false;
	}

	pfree(rpath);

	return collides;
}

/*
 * GetNewRelFileNode
 *		Generate a new relfilenode number that is unique within the given
 *		tablespace.
 *
 * If the relfilenode will also be used as the relation's OID, pass the
 * opened pg_class catalog, and this routine will guarantee that the result
 * is also an unused OID within pg_class.  If the result is to be used only
 * as a relfilenode for an existing relation, pass NULL for pg_class.
 * (in GPDB, 'pg_class' is unused, there is a different mechanism to avoid
 * clashes, across the whole cluster.)
 *
 * As with GetNewOid, there is some theoretical risk of a race condition,
 * but it doesn't seem worth worrying about.
 *
 * Note: we don't support using this in bootstrap mode.  All relations
 * created by bootstrap have preassigned OIDs, so there's no need.
 */
Oid
GetNewRelFileNode(Oid reltablespace, Relation pg_class, char relpersistence)
{
	RelFileNodeBackend rnode;
	bool		collides = true;
	BackendId	backend;

	switch (relpersistence)
	{
		case RELPERSISTENCE_TEMP:
			backend = TempRelBackendId;
			break;
		case RELPERSISTENCE_UNLOGGED:
		case RELPERSISTENCE_PERMANENT:
			backend = InvalidBackendId;
			break;
		default:
			elog(ERROR, "invalid relpersistence: %c", relpersistence);
			return InvalidOid;	/* placate compiler */
	}

	/* This logic should match RelationInitPhysicalAddr */
	rnode.node.spcNode = reltablespace ? reltablespace : MyDatabaseTableSpace;
	rnode.node.dbNode = (rnode.node.spcNode == GLOBALTABLESPACE_OID) ? InvalidOid : MyDatabaseId;

	/*
	 * The relpath will vary based on the backend ID, so we must initialize
	 * that properly here to make sure that any collisions based on filename
	 * are properly detected.
	 */
	rnode.backend = backend;

	do
	{
		CHECK_FOR_INTERRUPTS();

		/* Generate the Relfilenode */
		rnode.node.relNode = GetNewSegRelfilenode();

		if (!IsOidAcceptable(rnode.node.relNode))
			continue;

		collides = GpCheckRelFileCollision(rnode);

		if (!collides && rnode.node.spcNode != GLOBALTABLESPACE_OID)
		{
			/*
			 * GPDB_91_MERGE_FIXME: check again for a collision with a temp
			 * table (if this is a normal relation) or a normal table (if this
			 * is a temp relation).
			 *
			 * The shared buffer manager currently assumes that relfilenodes of
			 * relations stored in shared buffers can't conflict, which is
			 * trivially true in upstream because temp tables don't use shared
			 * buffers at all. We have to make this additional check to make
			 * sure of that.
			 */
			rnode.backend = (backend == InvalidBackendId) ? TempRelBackendId
														  : InvalidBackendId;
			collides = GpCheckRelFileCollision(rnode);
		}
	} while (collides);

	elog(DEBUG1, "Calling GetNewRelFileNode returns new relfilenode = %d", rnode.node.relNode);

	return rnode.node.relNode;
}
