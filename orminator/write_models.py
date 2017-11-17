import argparse

from orminator import ModelWriter


def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-o', '--output-fp', required=True)
    arg_parser.add_argument('-u', '--db-uri', required=True)

    args = arg_parser.parse_args()
    return args

def main():
    args = get_args()

    ModelWriter(db_uri=args.db_uri).write_models(output_fp=args.output_fp)


if __name__ == '__main__':
    main()
