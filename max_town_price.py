# MRJob program to calculate the maximum resale price per town from HDB resale dataset
from mrjob.job import MRJob

# Helper function to parse CSV lines into a list of columns
def parse_csv(line):
    return line.strip().split(",")

class MaxResalePrice(MRJob):

    def mapper(self, key, line):
        # Split the input line into columns
        cols = parse_csv(line)
        
        # Skip header or malformed rows
        if len(cols) < 11 or cols[0].lower() == "month":
            return
        
        try:
            # Extract town and resale price
            town = cols[1]  # 'town' column
            resale_price = int(cols[10])  # 'resale_price' column

            # Emit (town, resale_price) as key-value pair
            yield (town, resale_price)
        except:
            return # Skip lines with errors

    def reducer(self, key, values):
        """
        Reducer function that computes the maximum resale price
        for each town.
        """
        yield key, max(values)

if __name__ == "__main__":
    # Run the MRJob
    MaxResalePrice.run()
