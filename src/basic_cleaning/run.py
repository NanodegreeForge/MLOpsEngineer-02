#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
Author: Nguyen Chi Bach
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    # Dropping outlier
    logger.info("Dropping outlier")
    min_price = 10
    max_price = 350
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()

    # Dropping duplicates
    logger.info("Dropping duplicates")
    df = df.drop_duplicates().reset_index(drop=True)

    # Drop rows with improper geolocation
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    # Save to csv file
    logger.info("Save to csv file")
    df.to_csv(args.output_artifact, index=False)

    # Version control using W&B
    logger.info("Version control using W&B")
    artifact = wandb.Artifact(
     args.output_artifact,
     type=args.output_type,
     description=args.output_description,
    )
    artifact.add_file(args.output_artifact)
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="input artifact file",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="output artifact file",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="output artifact type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="output artifact description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="min price of cars in the dataset",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="max price of cars in the dataset",
        required=True
    )

    args = parser.parse_args()
    go(args)
