"""Python script that finds all *.proto files in a directory, and invokes protoc to compile them to
golang. Note that later this script will need to be extended to include other languages
"""
import pathlib
import argparse
import subprocess


def main():
    language_options = ['python', 'java']

    parser = argparse.ArgumentParser(
        description='Directory to look for protos')
    parser.add_argument('proto_dir', metavar='proto_dir', type=str,
                        help='Path to the directory containing the protos')
    parser.add_argument('language', metavar='language', type=str,
                        help=f'Laguage to compile protos to. Can be one of {language_options}')
    args = parser.parse_args()

    proto_dir_path = pathlib.Path(args.proto_dir)
    if not proto_dir_path.is_dir():
        raise ValueError(
            f"{args.proto_dir} either does not exist or is not a directory")

    if not args.language in language_options:
        raise ValueError(f"Cannot compile to language {args.language}")

    if args.language == "java":
        delete_files = "*.java"
        lite = "lite:"
    elif args.language == "python":
        delete_files = "*.py"
        lite=""

    for path in proto_dir_path.glob(delete_files):
        path.unlink()

    for path in proto_dir_path.glob("*.proto"):
        subprocess.run(
            f"protoc -I={args.proto_dir} --{args.language}_out={lite}{args.proto_dir} {path}", shell=True)


if __name__ == "__main__":
    main()
