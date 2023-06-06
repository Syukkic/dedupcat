import os
import hashlib
import shutil
import asyncio
import functools
from typing import List, Dict
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

from config import source_folder, dest_folder, img_extensions


def fetch_images_paths(root_directory: str) -> List[str]:
    """
    Iterate through the pictures folder and retrieve the absolute paths of images.
    """
    images_paths = []
    for subdir, _, files in os.walk(root_directory):
        for file_name in files:
            _, file_ext = os.path.splitext(file_name)
            if file_ext in img_extensions:
                images_paths.append(os.path.join(subdir, file_name))
    return images_paths


def calculate_hash_value(file_path: str) -> str:
    """
    Calculate the hash value of the given picture.
    """
    hash_value = hashlib.sha256()
    with open(file_path, "rb") as file:
        chunk = None
        while chunk != b"":
            chunk = file.read(1024)
            hash_value.update(chunk)
    return hash_value.hexdigest()


async def store_hash_values(all_images: List[str]) -> Dict[str, List[str]]:
    """
    Calculate the hash values of pictures and store them in a dictionary as keys,
    with their absolute paths as values in a list.
    """
    images_container = defaultdict(list)
    tasks = []

    loop = asyncio.get_event_loop()
    tasks = []
    with ProcessPoolExecutor() as pool:
        for image_path in all_images:
            tasks.append(
                loop.run_in_executor(
                    pool, functools.partial(calculate_hash_value, image_path)
                )
            )
        hashed_results = await asyncio.gather(*tasks)

    for idx, image_path in enumerate(all_images):
        images_container[hashed_results[idx]].append(image_path)

    return images_container


def pickup_images(images_with_hash: Dict[str, List[str]]) -> None:
    """
    Move unique pictures with a unique hash value to the destination folder,
    while maintaining their original subdirectories.
    Keep only one picture if duplicates exist.
    """
    for _, original_paths in images_with_hash.items():
        if len(original_paths) > 1:
            destination_path = dest_folder + "/".join(original_paths[0].split("/")[4:])
            destination_folder = os.path.dirname(destination_path)
            if not os.path.exists(destination_folder):
                os.makedirs(destination_folder)
            shutil.copy(original_paths[0], destination_path)
        else:
            destination_path = dest_folder + "/".join(original_paths[0].split("/")[4:])
            destination_folder = os.path.dirname(destination_path)
            if not os.path.exists(destination_folder):
                os.makedirs(destination_folder)
            shutil.copy(original_paths[0], destination_path)


if __name__ == "__main__":
    all_images = fetch_images_paths(source_folder)
    images_with_hash = asyncio.run(store_hash_values(all_images))
    pickup_images(images_with_hash)