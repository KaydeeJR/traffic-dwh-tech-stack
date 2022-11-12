from airflow.decorators import task


import logging
import pandas as pd

import os


def get_file_name(filePath) -> tuple:
    """
    Get file name to use as table name
    Parameter: filePath: str
    """
    fileName = os.path.basename(filePath).split('/')[-1]
    drone_no = fileName.split("_")[1]
    date = fileName.split("_")[0]
    time = fileName.split("_")[2]+(fileName.split("_")[3])
    return (drone_no, date, time.split(".")[0])


def get_trajectory_info(row_as_list) -> tuple:
    """
    converts the unstructured data into a structured data

    parameter: list

    split the string using ';'

    extract trajectory information available at index:
    0 -> track_id
    1 -> vehicle type
    2 -> traveled distance in m
    3 -> avg_speed in km/h
    """
    trajectory_info = []
    trajectory_info.append(row_as_list[0][2:])  # substring
    trajectory_info.append(row_as_list[1])
    trajectory_info.append(row_as_list[2])
    trajectory_info.append(row_as_list[3])
    return tuple(trajectory_info)


def get_vehicle_data(row_as_list):
    """
    generator function to convert the unstructured data into a structured data

    Parameter : dataframe row cast to list

    the last 6/10 columns are repeated every 6 columns based on the time frequency.
    For example, column_5 contains the latitude of the vehicle at time column_9, and column­­­_11 contains the latitude of the vehicle at time column_15
    """
    track_id = row_as_list[0].replace("['", "")
    time_index = 9  # first index of time frequency
    # go through the row as list
    while time_index < len(row_as_list):
        # fetch time frequency data and store in list
        time_freq = []
        time_freq.append(row_as_list[time_index-5])  # latitude
        time_freq.append(row_as_list[time_index-4])  # longitude
        time_freq.append(row_as_list[time_index-3])  # speed
        time_freq.append(row_as_list[time_index-2])  # long_acc
        time_freq.append(row_as_list[time_index-1])  # lat_acc
        time_freq.append(row_as_list[time_index])  # time-frequency
        time_freq.append(track_id)
        time_index = time_index + 6
        # generator object to store time frequency data
        yield (tuple(time_freq))


@task(task_id='load_data_to_db')
def load_trajectory_data(db_session, file_path):
    """
    for loop to insert data
    trajectory information includes:
    track_id, vehicle type, traveled distance, avg_speed in km/h
    """
    # read from CSV file - specify path
    df = pd.read_csv(filepath_or_buffer=file_path)

    # access dataframe values
    for data_index in range(len(df.values)):
        # list of data elements
        elements = str(df.values[data_index]).split(';')
        traj_info = get_trajectory_info(elements)
        file_info = get_file_name(filePath=file_path)
        values1 = traj_info+file_info
        command1 = f"INSERT INTO trajectories_data (track_id, vehicle_type, travelled_dist, avg_speed, drone_number, date, time_range) VALUES {values1};"
        try:
            db_session.execute(command1)
            db_session.commit()
        except Exception as ex:
            logging.error("Command skipped: ", command1)
            logging.error(ex)

        time_freq_info = get_vehicle_data(
            elements)  # generator object
        for time_data in time_freq_info:
            # loop through generator object
            command2 = f"INSERT INTO time_frequency_data (latitude, longitude, speed, long_acc, lat_acc, time, track_id) VALUES {time_data};"
            try:
                db_session.execute(command2)
                db_session.commit()
            except Exception as ex:
                logging.error("Command skipped: ", command1)
                logging.error(ex)

    # close the db session
    db_session.close()
