import logging
import time

from galileocontext.daemon import DefaultGalileoContextDaemon

logger = logging.getLogger(__name__)


def main():
    ctx_daemon = None
    try:
        ctx_daemon = DefaultGalileoContextDaemon('-deployment')
        logger.info("started daemon")
        ctx_daemon.start()
        time.sleep(2)
        ctx = ctx_daemon.context()
        logger.info("wait some seconds to let the services fill")
        time.sleep(10)
        logger.info("print some cluster details:")

        deployments = ctx.deployment_service.get_deployments()
        logger.info(deployments)
        containers = ctx.pod_service.get_pod_containers()
        logger.info(containers)
        nodes = ctx.node_service.get_nodes()
        logger.info(nodes)
        cpu = ctx.telemetry_service.get_node_cpu(nodes[0].name)
        logger.info(cpu)
        time.sleep(3)
        traces = ctx.trace_service.get_traces_api_gateway('eb-a-controller')
        logger.info(traces)

        logger.info("started everything, waiting for end...")
        input()
    except KeyboardInterrupt:
        logger.info("shutdown")
    finally:
        ctx_daemon.stop()


if __name__ == '__main__':
    main()
