from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from statistics import mean
from re import search

# Helper function to parse a CSV line into a list of values
def parse_csv(line):
    return line.strip().split(",")

class AveragePerSqmPrice(MRJob):
    # Set output protocol to RawValueProtocol to output plain strings without JSON encoding
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, key, line):
        # Split the input line into columns
        cols = line.strip().split(",")

        # Skip header or malformed rows
        if len(cols) < 11 or cols[0].lower() == "month":
            return

        try:
            # Extract year from 'month' column
            # E.g 2017 from '2017-01'
            year_match = search(r'\b(\d{4})\b', cols[0])
            if year_match:
                year = year_match.group(1)      # Extracted year
                town = cols[1]                  # 'town' column
                resale_price = int(cols[10])    # 'resale_price' column
                floor_area = float(cols[6])     # 'floor_area' column

                if floor_area == 0:
                    return # Skip rows where floor_area is zero to avoid division by zero
                
                # Calculate price per square meter
                per_sqm_price = resale_price / floor_area
                # Emit (year, town) as key and per_sqm_price as value
                yield (year, town), per_sqm_price 

        except (IndexError, ValueError, ZeroDivisionError):
            return # Skip lines with errors

    def reducer_init(self):
        self.header_emitted = False

    def reducer(self, key, values):
        """
        Reducer function that computes the average per sqm price
        for each year and town.
        """
        # Emit header once at the start of reduction
        if not self.header_emitted:
            yield None, "Year,Town,AveragePerSqmPrice"
            self.header_emitted = True

        # Unpack key into year and town
        year, town = key
        # Calculate the mean of all per_sqm_price values for this (year, town)
        avg_per_sqm = mean(values)
        # Emit result as a CSV-formatted string
        yield None, f"{year},{town},{avg_per_sqm:.2f}"

if __name__ == "__main__":
    # Run the MRJob
    AveragePerSqmPrice.run()
