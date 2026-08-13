#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import shutil
import tarfile
import tempfile
import urllib.request
from pathlib import Path, PurePosixPath


# Pin the archive so compatibility tests always use an immutable snapshot set.
TCK_REVISION = "d363b12d293b395d90abb42677f9ea63178dbc0d"
TCK_ARCHIVE_URL = (
    f"https://api.github.com/repos/apache/datasketches-tck/tarball/{TCK_REVISION}"
)
SUPPORTED_LANGUAGES = ("cpp", "go")


def download_archive(destination: Path) -> None:
    print(f"Downloading serialization snapshots from {TCK_ARCHIVE_URL}", flush=True)
    request = urllib.request.Request(
        TCK_ARCHIVE_URL,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "apache-datasketches-java",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        with destination.open("wb") as output:
            shutil.copyfileobj(response, output)


def extract_snapshots(archive_path: Path, languages: tuple[str, ...]) -> None:
    repository_root = Path(__file__).resolve().parents[1]
    serialization_data = repository_root / "serialization_test_data"
    serialization_data.mkdir(parents=True, exist_ok=True)

    staging_directories = {
        language: Path(
            tempfile.mkdtemp(
                prefix=f".{language}_generated_files-",
                dir=serialization_data,
            )
        )
        for language in languages
    }
    counts = dict.fromkeys(languages, 0)

    try:
        with tarfile.open(archive_path, mode="r:gz") as archive:
            for member in archive:
                if not member.isfile():
                    continue

                path = PurePosixPath(member.name)
                if path.suffix != ".sk":
                    continue

                language = next(
                    (
                        candidate
                        for candidate in languages
                        if path.parent.parts[-3:]
                        == ("serialization", candidate, "snapshots")
                    ),
                    None,
                )
                if language is None:
                    continue

                source = archive.extractfile(member)
                if source is None:
                    raise RuntimeError(f"could not read snapshot from archive: {path}")

                destination = staging_directories[language] / path.name
                if destination.exists():
                    raise RuntimeError(f"duplicate snapshot in archive: {path.name}")
                with source, destination.open("wb") as output:
                    shutil.copyfileobj(source, output)
                counts[language] += 1

        for language, count in counts.items():
            if count == 0:
                raise RuntimeError(
                    f"no {language} snapshots found in the TCK archive"
                )

        for language, staging_directory in staging_directories.items():
            destination = serialization_data / f"{language}_generated_files"
            if destination.is_symlink():
                raise RuntimeError(
                    f"snapshot output path cannot be a symbolic link: {destination}"
                )
            if destination.exists():
                if not destination.is_dir():
                    raise RuntimeError(
                        f"snapshot output path is not a directory: {destination}"
                    )
                shutil.rmtree(destination)
            staging_directory.replace(destination)
            print(
                f"Extracted {counts[language]} {language} snapshots into {destination}"
            )
    finally:
        for staging_directory in staging_directories.values():
            if staging_directory.exists():
                shutil.rmtree(staging_directory)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download serialization snapshots from apache/datasketches-tck."
    )
    parser.add_argument(
        "languages",
        choices=SUPPORTED_LANGUAGES,
        metavar="LANG",
        nargs="*",
        help="languages to download (cpp and go by default)",
    )
    args = parser.parse_args()
    languages = tuple(dict.fromkeys(args.languages or SUPPORTED_LANGUAGES))

    with tempfile.TemporaryDirectory(prefix="datasketches-tck-") as temp_directory:
        archive_path = Path(temp_directory) / "datasketches-tck.tar.gz"
        download_archive(archive_path)
        extract_snapshots(archive_path, languages)


if __name__ == "__main__":
    main()
