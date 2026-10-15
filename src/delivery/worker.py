"""Explicit integration boundary; never reports simulated work as delivered."""
import argparse

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('operation',choices=['copy-batch','snowflake'])
    parser.add_argument('run_id')
    parser.add_argument('--client')
    parser.add_argument('--manifest-uri')
    args=parser.parse_args()
    raise SystemExit(f'Worker integration required for {args.operation}; see docs/contracts.md')
if __name__=='__main__':
    main()
