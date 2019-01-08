#!/usr/bin/env python3
"""Author:      Olivier van der Toorn <o.i.vandertoorn@utwente.nl>
Description:    Example implementation of an ImpalaClient.
"""
import sys
import pickle
from sql.connectors.impala import ImpalaClient


def main():
    """Main function, reads flags, builds a client and queries the cluster.
    """
    impala_client = ImpalaClient()
    line = sys.stdin.read()
    if line:
        query = line

    else:
        sys.exit(1)

    with impala_client as client:
        result = client.query(query)

    print(pickle.dumps(result))


if __name__ == "__main__":
    main()
