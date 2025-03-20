import os
import json
import time
import random
import datetime
from city_set import city_list
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing


def get_destination_city(org_city):
    # Select a random city from the list
    city = random.sample(city_list, 1)[0]
    while city == org_city:
        city = random.sample(city_list, 1)[0]
    return city


def get_date(month):
    # Create a random date for this month and year
    if month == 2:
        day = random.randint(1, 28)
    elif month in [4, 6, 9, 11]:
        day = random.randint(1, 30)
    else:
        day = random.randint(1, 31)

    if random.random() < random.uniform(0.03, 0.05):
        return None

    return datetime.datetime(2025, month, day, random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))


def create_files(date, org_city):
    # Create a file with the date and city name
    file_name = f"{date}_{org_city}.txt"
    with open(file_name, "w") as f:
        f.write(f"City: {org_city}\nDate: {date}")


def get_duration():
    # Get random duration
    if random.random() < random.uniform(0.03, 0.05):
        return None
    duration = random.randint(14000, 64000)
    return duration


def get_passenger():
    # Get random duration
    if random.random() < random.uniform(0.03, 0.05):
        return None
    duration = random.randint(75, 640)
    return duration


def get_flight_details(org_city, dest_city, date_time, duration, pasanger):
    flight = {
        "origin_city": org_city,
        "destination_city": dest_city,
        "date_time": str(date_time),
        "flight_duration_secs": duration,
        "passenger_on_board": pasanger
    }
    return flight


def create_flight_folder(date, org_city):
    # Create a file and store in to the folder ...temp/flights
    folder_path = "temp/flights"
    os.makedirs(folder_path, exist_ok=True)

    # Define the file path
    file_name = f"{date}_{org_city}.json"
    file_path = os.path.join(folder_path, file_name)

    return file_path


def write_flight_details(org_city, month):
    json_data = []
    t_object = random.randint(40, 50)

    for i in range(t_object):
        dest_city = get_destination_city(org_city)
        date_time = get_date(month)
        duration = get_duration()
        pasanger = get_passenger()
        # Get flight details
        flight = get_flight_details(org_city, dest_city, date_time, duration, pasanger)
        json_data.append(flight)

    # Create a file
    year = random.choice([2023, 2024, 2025])
    file_path = create_flight_folder(f'{month}-{year}', org_city)
    with open(file_path, "w") as f:
        json.dump(json_data, f, indent=4)


def writer_worker(org_city, num_files):
    # org_city = get_origin_city()
    for _ in range(num_files):
        month = random.randint(1, 12)
        write_flight_details(org_city, month)


def track_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Start time
        result = func(*args, **kwargs)  # Execute the function
        end_time = time.time()  # End time
        print(f"Total {end_time - start_time:.4f} seconds is taken to creating the files")
        return result

    return wrapper


@track_execution_time
def main():
    print(f"Total cities: {len(city_list)}")

    total_worker = multiprocessing.cpu_count() * 4
    tasks = []

    with ThreadPoolExecutor(max_workers=total_worker) as executor:

        #    futures = [executor.submit(writer_worker,city) for city in cities]

        for city in city_list:
            num_files = random.randint(40, 60)
            print(f"City: {city}, Number of files: {num_files}")
            tasks.append(executor.submit(writer_worker, city, num_files))

    for future in as_completed(tasks):
        future.result()


if __name__ == "__main__":
    main()
