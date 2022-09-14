from galileocontext.context.cluster import Context, ClusterContext


def create_cluster_context(ctx: Context):
    return ClusterContext(ctx)
