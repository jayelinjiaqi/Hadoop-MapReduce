# MRJob program to calculate the average remaining lease (in years) as of 2025 per town
from mrjob.job import MRJob
from statistics import mean
import re

# Helper function to parse a CSV line into a list of values
def parse_csv(line):
    return line.strip().split(",")

class AverageRemainingLease(MRJob):
    def mapper(self, key, line):
        # Split the input line into columns
        cols = parse_csv(line)

        # Skip header or malformed rows
        if len(cols) < 11 or cols[0].lower() == "month":
            return

        try:
            town = cols[1] # 'town' column
            remaining_lease_str = cols[9]  # 'remaining_lease' column
            
            # Extract years and months using regex (eg. remaining_lease = 69 years 11 months)
            match = re.match(r'(\d+)\s+years\s+(\d+)\s+months', remaining_lease_str)
            if match:
                years = int(match.group(1))
                months = int(match.group(2))
                lease_in_years = years + (months / 12)
                
                # Emit town as key, and lease_in_years as value
                yield (town, lease_in_years)
        
        except Exception:
            return  # Skip lines with errors

    def reducer(self, key, values):
        """
        Reducer function that computes the average lease reamining
        for each town.
        """
        # Calculate and emit the average remaining lease per town
        yield key, round(mean(values), 2)

if __name__ == "__main__":
    # Run the MRJob
    AverageRemainingLease.run()
