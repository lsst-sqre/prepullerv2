import argparse
import json
import logging
import sys
from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException
from .scanrepo import ScanRepo


class Prepuller(object):
    repo = None
    logger = None
    client = None
    args = argparse.Namespace(debug=False,
                              json=True,
                              repo="hub.docker.com",
                              owner="lsstsqre",
                              name="jld-lab",
                              dailies=3,
                              weeklies=2,
                              releases=1,
                              insecure=False,
                              sort="comp_ts",
                              path="/v2/repositories/lsstsqre/jld-lab/tags")

    def __init__(self, args=None):
        logging.basicConfig()
        self.logger = logging.getLogger(__name__)
        if args:
            self.args = args
        if self.args and self.args.debug:
            self.logger.setLevel(logging.DEBUG)
            self.logger.debug("Debug logging on.")
        else:
            self.logger.setLevel(logging.INFO)
        try:
            config.load_incluster_config()
        except ConfigException:
            try:
                config.load_kube_config()
            except Exception:
                logging.critical(sys.argv[0], " must be run from a system",
                                 " with k8s API access.")
                raise
        self.client = client.CoreV1Api()
        self.logger.debug("Arguments: %s" % str(args))
        self.repo = ScanRepo(host=args.repo, path=args.path,
                             owner=args.owner, name=args.name,
                             dailies=args.dailies, weeklies=args.weeklies,
                             releases=args.releases,
                             json=True, insecure=args.insecure,
                             sort_field=args.sort, debug=args.debug)
        self.logger.debug("Scanning '%s' for prepull images" % args.repo)
        self.repo.scan()
        self.logger.debug("Repository data: %s" % json.dumps(self.repo.data,
                                                             sort_keys=True,
                                                             indent=4))
