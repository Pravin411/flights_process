import os
import json
import time
import random
import threading
import pandas as pd
import numpy as np
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed

folder_path = "temp/flights"


def read_from_json_file(file_list):
    # Process the file with memory efficient way like generator
    for file in file_list:
        file_path = os.path.join(folder_path, file)
        with open(file_path, "r") as f:
            data = json.load(f)
            yield data


def file_processing(files):
    # Read all json files form the folder folder_path
    data_list = []
    for data in read_from_json_file(files):
        df = pd.DataFrame(data)
        data_list.append(df)
    return data_list


def data_processing(files):
    data_list = file_processing(files)
    df = pd.concat(data_list, ignore_index=True)

    total_records = len(df)
    total_dirty_records = df.isnull().sum().sum()
    highest_duration = df["flight_duration_secs"].max()

    city_flight_counts = df["destination_city"].value_counts().reset_index()
    city_flight_counts.columns = ["destination_city", "num_flights"]

    top_25_cities = city_flight_counts.head(25)["destination_city"]

    city_stats = df[df["destination_city"].isin(top_25_cities)].groupby("destination_city").agg(
        avg_flight_duration=("flight_duration_secs", "mean"),
        p95_flight_duration=("flight_duration_secs", lambda x: x.quantile(0.95))
    ).reset_index()

    # Track passenger arrivals (destination_city) and departures (origin_city)
    passenger_arrivals = df.groupby("destination_city")["passenger_on_board"].sum().reset_index()
    passenger_departures = df.groupby("origin_city")["passenger_on_board"].sum().reset_index()

    passenger_arrivals.columns = ["city", "total_arrivals"]
    passenger_departures.columns = ["city", "total_departures"]

    # add above values to the result dictionary
    result = {
        "total_records": total_records,
        "total_dirty_records": total_dirty_records,
        "highest_duration": highest_duration,
        "city_flight_counts": city_flight_counts,
        "city_stats": city_stats,
        "passenger_arrivals": passenger_arrivals,
        "passenger_departures": passenger_departures
    }

    return result


def worker_distribution():
    files_list = os.listdir(folder_path)
    total_files = len(files_list)

    # Ideal threads2-4 cpu cores like cores * 2 or cores * 4
    # total_worker = os.cpu_count() * 4

    num_workers = min(multiprocessing.cpu_count() * 4, total_files)

    chunk_size = max(1, total_files // num_workers)
    print(f"Total files: {total_files}, Number of workers: {num_workers}, Chunk size: {chunk_size}")

    file_chunks = [files_list[i:i + chunk_size] for i in range(0, total_files, chunk_size)]

    return file_chunks


def track_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Start time
        result = func(*args, **kwargs)  # Execute the function
        end_time = time.time()  # End time
        print(f"Total {end_time - start_time:.4f} seconds is taken to processing all files")
        return result

    return wrapper


@track_execution_time
def main():
    file_chunks = worker_distribution()
    results = []
    with ThreadPoolExecutor(max_workers=len(file_chunks)) as executor:
        futures = [executor.submit(data_processing, chunk) for chunk in file_chunks]

        for future in as_completed(futures):
            results.append(future.result())

        # Merge results from all threads
    combined_df = pd.concat([res["city_flight_counts"] for res in results], ignore_index=True)
    all_city_stats = pd.concat([res["city_stats"] for res in results], ignore_index=True)

    # Merge passenger data from all threads
    combined_arrivals = pd.concat([res["passenger_arrivals"] for res in results], ignore_index=True)
    combined_departures = pd.concat([res["passenger_departures"] for res in results], ignore_index=True)

    # Compute final total records & dirty records
    final_total_records = sum(res["total_records"] for res in results)
    final_dirty_records = sum(res["total_dirty_records"] for res in results)

    # Compute final aggregated city flight counts
    final_city_flight_counts = combined_df.groupby("destination_city")["num_flights"].sum().reset_index()

    # Get top 25 cities by number of flights
    top_25_cities = final_city_flight_counts.nlargest(25, "num_flights")["destination_city"]

    # Compute final AVG & P95 flight durations for top 25 cities
    final_stats = all_city_stats[all_city_stats["destination_city"].isin(top_25_cities)].groupby(
        "destination_city").agg(
        avg_flight_duration=("avg_flight_duration", "mean"),
        p95_flight_duration=("p95_flight_duration", "mean")
    ).reset_index()

    # Compute total arrivals and departures per city
    final_arrivals = combined_arrivals.groupby("city")["total_arrivals"].sum().reset_index()
    final_departures = combined_departures.groupby("city")["total_departures"].sum().reset_index()

    # Identify the top 2 cities for arrivals & departures
    top_2_arrivals = final_arrivals.nlargest(2, "total_arrivals")
    top_2_departures = final_departures.nlargest(2, "total_departures")

    # Print Final Results
    print(f"Total Records Processed: {final_total_records}")
    print(f"Total Dirty Records (NULLs): {final_dirty_records}")
    print(f"Top 2 Cities with Highest Passenger Arrivals: {top_2_arrivals}")
    print(f"Top 2 Cities with Highest Passenger Departures: {top_2_departures}")
    print(f"Final Statistics for Top 25 Cities:\n{final_stats}")


if __name__ == "__main__":
    main()
