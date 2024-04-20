import matplotlib.pyplot as plt

def parse_file_to_lists(file_path, y1, y2, y3, y4):
    """
    Parses a file and appends the values to the given lists.
    
    Parameters:
    - file_path: str, the path to the file.
    - y1, y2, y3, y4: lists, the lists to which the values will be appended.
    """
    with open(file_path, 'r') as file:
        for line in file:
            # Strip newline characters and split the line into parts
            parts = line.strip().split()
            
            # Ensure the line has exactly four elements
            if len(parts) == 4:
                # Convert strings to floats and append to corresponding lists
                y1.append(float(parts[0]))
                y2.append(float(parts[1]))
                y3.append(float(parts[2]))
                y4.append(float(parts[3]))
            else:
                print(f"Warning: Line in {file_path} does not contain exactly four elements and has been skipped.")

# get data
x = [10, 50, 100, 500, 1000]
y1, y2, y3, y4 = [], [], [], []
file_paths = [
    "resultsData/naive_knn_10",
    "resultsData/naive_knn_50",
    "resultsData/naive_knn_100",
    "resultsData/naive_knn_500",
    "resultsData/naive_knn_1000"
]
for path in file_paths:
    parse_file_to_lists(path, y1, y2, y3, y4)

# create a 2x2 grid of subplots
fig, axs = plt.subplots(2, 2)  # 2 rows, 2 columns

# plotting on each subplot
axs[0, 0].plot(x, y1, 'tab:red')
axs[0, 0].set_title('Put Time')

axs[0, 1].plot(x, y2, 'tab:blue')
axs[0, 1].set_title('Put Throughput')

axs[1, 0].plot(x, y3, 'tab:green')
axs[1, 0].set_title('Get Time')

axs[1, 1].plot(x, y4, 'tab:orange')
axs[1, 1].set_title('Get Throughput')

# Adding a title to the figure
fig.suptitle('Naive Nearest K Neighbors')

# Automatically adjust layout
plt.tight_layout(rect=[0, 0, 1, 0.95])  # Adjust the rect so the title does not overlap with subplots

# Show the plots
plt.show()