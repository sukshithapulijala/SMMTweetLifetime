import sqlite3
import matplotlib.pyplot as plt
import numpy as np


def read_collected_data():
    query = "SELECT * FROM tweets_classified WHERE type NOT NULL ORDER BY count_day1 DESC LIMIT 10"
    connection = sqlite3.connect("twitter.db")
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()

    return results


def plot_data(t_data):
    labels = ['Nov 17-18', 'Nov 18-19', 'Nov 19-20', 'Nov 20-21', 'Nov 21-22', 'Nov 22-23', 'Nov 23-24']
    x = np.arange(len(labels))  # the label locations
    width = 0.5  # the width of the bars

    fig, ax = plt.subplots()

    t_plot_data = []
    rect = []
    for i in range(len(t_data)):
        t_plot_data.append([t_data[i][1], t_data[i][2], t_data[i][3], t_data[i][4],
                            t_data[i][5], t_data[i][6], t_data[i][7]])
        rect.append(ax.bar(x - (width * (5-i) / 10), t_plot_data[i], width/10, label=t_data[i][0]))

        # if i is 0:
        #     rect.append( ax.bar(x - width / 2, t_plot_data[i], width, label=t_data[i][0]))
        # else:
        #     rect.append(ax.bar(x + width / 2, t_plot_data[i], width, label=t_data[i][0]))

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Number of tweets')
    ax.set_title('Number of tweets per day')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    def auto_label(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height/1000),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

    # Enable this if you want number of tweets to be displayed on each bar
    # for i in range(len(t_data)):
    #     auto_label(rect[i])

    fig.tight_layout()

    plt.show()


if __name__ == "__main__":
    twitter_data = read_collected_data()
    plot_data(twitter_data)
