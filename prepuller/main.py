#!/usr/bin/env python3
import argparse
from .prepuller import Prepuller


def standalone():
    args = parse_args()
    prepuller = Prepuller(args=args)
    prepuller.repo.scan()


def parse_args():
    """Parse command-line arguments"""
    desc = "Scan Docker repo for prepull images."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("-d", "--debug", action="store_true",
                        help="enable debugging")
    parser.add_argument("-r", "--repo", "--repository",
                        help="repository host [hub.docker.com]",
                        default="hub.docker.com")
    parser.add_argument("-o", "--owner", "--organization", "--org",
                        help="repository owner [lsstsqre]",
                        default="lsstsqre")
    parser.add_argument("-n", "--name",
                        help="repository name [jld-lab]",
                        default="jld-lab")
    parser.add_argument("-q", "--dailies", "--daily", "--quotidian", type=int,
                        help="# of daily builds to keep [3]",
                        default=3)
    parser.add_argument("-w", "--weeklies", "--weekly", type=int,
                        help="# of weekly builds to keep [2]",
                        default=2)
    parser.add_argument("-b", "--releases", "--release", type=int,
                        help="# of release builds to keep [1]",
                        default=1)
    parser.add_argument("-i", "--insecure", "--no-tls", "--no-ssl",
                        help="Do not use TLS to connect [False]",
                        type=bool,
                        default=False)
    parser.add_argument("-s", "--sort", "--sort-field", "--sort-by",
                        help="Field to sort results by [comp_ts]",
                        default="comp_ts")
    results = parser.parse_args()
    results.path = ("/v2/repositories/" + results.owner + "/" +
                    results.name + "/tags")
    return results

if __name__ == "__main__":
    standalone()
