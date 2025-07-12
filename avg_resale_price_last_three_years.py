# MRJob program to calculate average resale price of each flat type in the last 3 years

from mrjob.job import MRJob
from statistics import mean
from datetime import datetime

# Helper function to parse a CSV line into a list of values
def parse_csv(line):
    return line.strip().split(",")

class AverageResalePriceLastThreeYears(MRJob):

    def mapper(self, _, line):

        # Split the input line into columns
        cols = parse_csv(line)

        # Skip header or malformed rows
        if len(cols) < 11 or cols[0].lower() == "month":
            return

        try:
            month_str = cols[0]          # 'month' column
            flat_type = cols[2]          # 'flat_type' column 
            resale_price = int(cols[10]) # 'resale_price' column

            # Extract the year as an integer (e.g. "2022-05")
            year = int(month_str.split("-")[0]) 

            # Get the current year
            current_year = datetime.now().year

            # Include only those within the last 3 years
            # e.g. include years 2022 - 2024 when script executed in year 2025
            if (current_year - 3) <= year <= (current_year - 1):
                yield (flat_type, resale_price)

        except (ValueError, IndexError):
            return # Skip lines with errors

    def reducer(self, flat_type, resale_prices):
        """
        Reducer function that computes the average resale price
        for each flat type.
        """
        prices = list(resale_prices)
        if prices:
            # Calculate the mean and round to 2 decimal places
            yield flat_type, round(mean(prices), 2)

if __name__ == "__main__":
    # Run the MRJob
    AverageResalePriceLastThreeYears.run()
