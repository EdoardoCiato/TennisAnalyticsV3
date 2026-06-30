import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.patches import Polygon

IMAGE_PATH = 'data/templates/image.png'

def onclick(event):
    print( event.xdata, event.ydata)


def main():
    img = mpimg.imread(IMAGE_PATH)
    # fig = the figure, the entire window
    # ax = Axes, where you actually draw
    # we need both because a figure may contain multiple axes. 
    fig, ax = plt.subplots(figsize=(30, 30 * 808 / 1947))
    ax.imshow(img)
    ax.axis('off')
    # canvas = the surface receiving mouse and keyboard events. 
    # mpl_connect = connects an event to a function
    # button_press_event = event name, onclick = chosen function when the event occurs. 
    # cid = connection ID. the connection between the event and the function. 
    cid = fig.canvas.mpl_connect("button_press_event", onclick)

    zones = {
    "deuce_T": [
        (978, 411),   # bottom-left
        (977, 210),   # top-left
        (1112, 210),  # top-right
        (1168, 411),  # bottom-right
    ],

    "deuce_middle": [
        (1168, 411),  # bottom-left
        (1112, 210),  # top-left
        (1260, 207),  # top-right
        (1370, 411),  # bottom-right
    ],

    "deuce_wide": [
        (1370, 411),  # bottom-left
        (1260, 207),  # top-left
        (1400, 204),  # top-right
        (1576, 411),  # bottom-right
    ],

    "ad_wide": [
        (368, 411),
        (574, 411),
        (684, 205),
        (544, 204),
    ],

    "ad_middle": [
        (574, 411),
        (684, 205),
        (832, 207),
        (776, 411),
    ],

    "ad_T": [
        (832, 207),
        (776, 411),
        (966, 411),
        (967, 210),
    ],
}
    for name, points in zones.items():
            polygon = Polygon(points,
            closed=True,
            edgecolor= 'white',
            linewidth=3, )

            ax.add_patch(polygon)





    plt.show()

main()