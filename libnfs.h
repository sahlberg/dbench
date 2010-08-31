
struct nfsio *nfsio_connect(const char *server, const char *export, const char *protocol, int initial_xid, int xid_stride);
void nfsio_disconnect(struct nfsio *nfsio);
nfsstat3 nfsio_getattr(struct nfsio *nfsio, const char *name, fattr3 *attributes);
nfsstat3 nfsio_lookup(struct nfsio *nfsio, const char *name, fattr3 *attributes);
nfsstat3 nfsio_access(struct nfsio *nfsio, const char *name, uint32 desired, uint32 *access);
nfsstat3 nfsio_create(struct nfsio *nfsio, const char *name);
nfsstat3 nfsio_remove(struct nfsio *nfsio, const char *name);
nfsstat3 nfsio_write(struct nfsio *nfsio, const char *name, char *buf, uint64_t offset, int len, int stable);
nfsstat3 nfsio_commit(struct nfsio *nfsio, const char *name);
nfsstat3 nfsio_read(struct nfsio *nfsio, const char *name, char *buf, uint64_t offset, int len, int *count, int *eof);
nfsstat3 nfsio_fsinfo(struct nfsio *nfsio);
nfsstat3 nfsio_fsstat(struct nfsio *nfsio);
nfsstat3 nfsio_pathconf(struct nfsio *nfsio, char *name);
nfsstat3 nfsio_symlink(struct nfsio *nfsio, const char *old, const char *new);
nfsstat3 nfsio_link(struct nfsio *nfsio, const char *old, const char *new);
nfsstat3 nfsio_readlink(struct nfsio *nfsio, char *name, char **link_name);
nfsstat3 nfsio_mkdir(struct nfsio *nfsio, const char *name);
nfsstat3 nfsio_rmdir(struct nfsio *nfsio, const char *name);
nfsstat3 nfsio_rename(struct nfsio *nfsio, const char *old, const char *new);

typedef void (*nfs3_dirent_cb)(struct entryplus3 *e, void *private_data);
nfsstat3 nfsio_readdirplus(struct nfsio *nfsio, const char *name, nfs3_dirent_cb cb, void *private_data);
nfsstat3 nfsio_readdir(struct nfsio *nfsio, const char *name);

const char *nfs_error(int error);
