from mrjob.job import MRJob
from re import search  # Imported but not used here

# Helper function to parse a CSV line into a list of values
def parse_csv(line):
    return line.strip().split(",")

class NumThreeRoomTown(MRJob):

    def mapper(self, _, line):
        # Split the input line into columns
        cols = parse_csv(line)

        # Skip header or malformed rows
        if len(cols) < 11 or cols[0].lower() == "month":
            return
        
        try:
            # Check if transaction is from year 2024 and flat type is 3 ROOM
            if "2024" in cols[0] and cols[2] == "3 ROOM":
                town = cols[1]  # 'town' column
                yield town, 1   # Emit count of 1 for each 3 ROOM flat found
        except (IndexError, ValueError):
            return # Skip lines with errors

    def reducer(self, key, values):
        """
        Reducer function that computes the number of three-room flats transacted
        in 2024.
        """
        yield key, sum(values)

if __name__ == "__main__":
    # Run the MRJob
    NumThreeRoomTown.run()
