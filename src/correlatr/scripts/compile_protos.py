"""Python script that finds all *.proto files in a directory, and invokes protoc to compile them to
python
"""
import pathlib
import argparse
import subprocess

def main():
    parser = argparse.ArgumentParser(
        description='Directory to look for protos')
    parser.add_argument('proto_dir', metavar='proto_dir', type=str,
                        help='Path to the directory containing the protos')
    args = parser.parse_args()

    proto_dir_path = pathlib.Path(args.proto_dir)
    if not proto_dir_path.is_dir():
        raise ValueError(
            f"{args.proto_dir} either does not exist or is not a directory")

    for path in proto_dir_path.glob("*.py"):
        path.unlink()

    for path in proto_dir_path.glob("*.proto"):
        subprocess.run(
            f"protoc -I={args.proto_dir} --python_out={args.proto_dir} {path}", shell=True)

if __name__ == "__main__":
    main()
