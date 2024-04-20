import matplotlib.pyplot as plt

def parse_results_file(file_path, y1, y2, y3, y4):
    
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split()
            y1.append(float(parts[0])) # put time
            y2.append(float(parts[1])) # put throughput
            y3.append(float(parts[2])) # get time
            y4.append(float(parts[3])) # get throughput

if __name__ == "__main__":
    
    # parse results files to get x and y values
    x = [10, 50, 100, 500, 1000]
    y1, y2, y3, y4 = [], [], [], []
    num_file_items = 500
    file_paths = ["resultsData/naive_knn_10", 
                  "resultsData/naive_knn_50", 
                  "resultsData/naive_knn_100", 
                  "resultsData/naive_knn_500", 
                  "resultsData/naive_knn_1000"]
    for fp in file_paths:
        parse_results_file(fp, y1, y2, y3, y4)

    # make plots
    fig, axs = plt.subplots(2, 2)  
    
    axs[0, 0].plot(x, y1, '.-', color = 'tab:red')
    axs[0, 0].set_title('Put Time')

    axs[0, 1].plot(x, y2, '.-', color = 'tab:blue')
    axs[0, 1].set_title('Put Throughput')

    axs[1, 0].plot(x, y3, '.-', color = 'tab:green')
    axs[1, 0].set_title('Get Time')

    axs[1, 1].plot(x, y4, '.-', color = 'tab:orange')
    axs[1, 1].set_title('Get Throughput')

    fig.suptitle(f"Naive Nearest K Neighbors (num_file_items = {num_file_items})")
    plt.tight_layout(rect=[0, 0, 1, 0.95])  # Adjust the rect so the title does not overlap with subplots
    plt.show()