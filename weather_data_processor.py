import re
import numpy as np
import pandas as pd
import logging
from data_ingestion import read_from_web_CSV


### START FUNCTION 
class WeatherDataProcessor:
    """
    A class for processing weather data.

    Parameters:
    config_params (dict): A dictionary containing configuration parameters including weather CSV path and regex patterns.
    logging_level (str): Optional. Specifies the logging level for the class. Defaults to "INFO".

    Methods:
    initialize_logging: Initializes logging settings for the class.
    weather_station_mapping: Loads weather station data from the web and assigns it to weather_df.
    extract_measurement: Extracts measurement values from text messages using regex patterns.
    process_messages: Processes text messages to extract measurements and updates the DataFrame accordingly.
    calculate_means: Calculates mean values of measurements grouped by weather station and type.
    process: Executes the data processing steps in sequence.

    Returns:
    pandas.DataFrame: The processed weather DataFrame.
    """
    
    def __init__(self, config_params, logging_level="INFO"):
        """
        Initializes the WeatherDataProcessor class with configuration parameters and logging settings.
        """
        self.weather_station_data = config_params['weather_csv_path']
        self.patterns = config_params['regex_patterns']
        self.weather_df = None
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of WeatherDataProcessor.
        """
        logger_name = __name__ + ".WeatherDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False

        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO

        self.logger.setLevel(log_level)

        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def weather_station_mapping(self):
        """
        Loads weather station data from the web and assigns it to weather_df.
        """
        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data from the web.") 

    def extract_measurement(self, message):
        """
        Extracts measurement values from text messages using regex patterns.
        """
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                self.logger.debug(f"Measurement extracted: {key}")
                return key, float(next((x for x in match.groups() if x is not None)))
        self.logger.debug("No measurement match found.")
        return None, None

    def process_messages(self):
        """
        Processes text messages to extract measurements and updates the DataFrame accordingly.
        """
        if self.weather_df is not None:
            result = self.weather_df['Message'].apply(self.extract_measurement)
            self.weather_df['Measurement'], self.weather_df['Value'] = zip(*result)
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning("weather_df is not initialized, skipping message processing.")
        return self.weather_df

    def calculate_means(self):
        """
        Calculates mean values of measurements grouped by weather station and type.
        """
        if self.weather_df is not None:
            means = self.weather_df.groupby(by=['Weather_station_ID', 'Measurement'])['Value'].mean()
            self.logger.info("Mean values calculated.")
            return means.unstack()
        else:
            self.logger.warning("weather_df is not initialized, cannot calculate means.")
            return None
    
    def process(self):
        """
        Executes the data processing steps in sequence.
        """
        self.weather_station_mapping()
        self.process_messages()
        self.logger.info("Data processing completed.")
        return self.weather_df

### END FUNCTION
