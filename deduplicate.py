import os
import hashlib
import shutil
from typing import List, Dict
from collections import defaultdict

from rich.progress import track
from config import source_folder, dest_folder, img_extensions


def fetch_images_paths(rootdir: str) -> List[str]:
    """
    iterate pictures folder and get images absolute path
    """

    images_paths = []
    for subdir, _, files in track(os.walk(rootdir)):
        for file in files:
            _, file_ext = os.path.splitext(file)
            if file_ext in img_extensions:
                images_paths.append(os.path.join(subdir, file))

    return images_paths


def hash_file(filename: str) -> str:
    """
    param: The path of pictures
    return: Hash value of pictures
    """

    hash_value = hashlib.sha1()
    with open(filename, "rb") as file:
        chunk = None
        while chunk != b"":
            chunk = file.read(1024)
            hash_value.update(chunk)

    return hash_value.hexdigest()


def store_images_hash() -> Dict[str, List[str]]:
    """
    Calculate pictures hash value and store into dict as key,
    their absolute path as value in list
    """

    imgs_container = defaultdict(list)
    all_images = fetch_images_paths(source_folder)
    for img in track(all_images):
        hash_value = hash_file(img)
        imgs_container[hash_value].append(img)

    return imgs_container


def pickup_images() -> None:
    """
    Pick up unique-hash-value picture to the new folder,
    and keep their original subdirectories.

    Keep one picture if they are duplicated.
    """
    images_with_hash = store_images_hash()
    for _, orig_paths in track(images_with_hash.items()):
        if len(orig_paths) > 1:
            dest_path = dest_folder + "/".join(orig_paths[0].split("/")[4:])
            dst_folder = os.path.dirname(dest_path)
            if not os.path.exists(dst_folder):
                os.makedirs(dst_folder)
            shutil.copy(orig_paths[0], dest_path)
        else:
            dest_path = dest_folder + "/".join(orig_paths[0].split("/")[4:])
            dst_folder = os.path.dirname(dest_path)
            if not os.path.exists(dst_folder):
                os.makedirs(dst_folder)
            shutil.copy(orig_paths[0], dest_path)


if __name__ == "__main__":
    pickup_images()
