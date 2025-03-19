import itertools


def ansi_color(color, text):
    return f"\x1b[38;5;{color}m{text}\x1b[0m"


ansi_heatmap_color = [
    16,
    17,
    17,
    18,
    19,
    20,
    20,
    21,
    27,
    27,
    33,
    39,
    45,
    45,
    51,
    50,
    50,
    49,
    48,
    47,
    47,
    46,
    82,
    82,
    118,
    154,
    190,
    190,
    226,
    220,
    220,
    214,
    208,
    202,
    202,
    196,
    197,
    197,
    198,
    199,
    200,
    200,
    201,
]

block_char_v = ["░", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"]
block_char_h = ["░", "▏", "▎", "▍", "▌", "▋", "▊", "▉", "█"]
char = ["╭", "╮", "│", "╰", "╯", "─"]


def ratio_matrix_width_high(data, w, h, prefix=""):
    idxs = [0 if v == -1 else int(v * 8) for v in data]
    texts = [block_char_v[idx] for idx in idxs]
    ic = itertools.chain(iter(texts), itertools.repeat(" "))
    line = w * char[-1]
    top = prefix + f"{char[0]}{line}{char[1]}"
    bottom = " " * len(prefix) + f"{char[3]}{line}{char[4]}"
    print(top)
    for i in range(h):
        l = f"{char[2]}{''.join([next(ic) for i in range(w)])}{char[2]}"
        if prefix != "":
            l = str(i * w).rjust(len(prefix)) + l
        print(l)
    print(bottom)


def heatmap_matrix_width_high(data, w, h, prefix=""):
    max_data, min_data = max(data), min(data)
    step = (max_data - min_data) / len(ansi_heatmap_color)

    def f(v):
        vv = int((v - min_data) / step)
        return vv if vv < len(ansi_heatmap_color) else vv - 1

    colors = [ansi_color(ansi_heatmap_color[f(v)], block_char_h[-1]) for v in data]

    ic = itertools.chain(iter(colors), itertools.repeat(" "))
    line = w * char[-1]
    # top_legend = " " * 2 + "0" + " " * (w - 1) + str(w)
    # print(top_legend)
    top = prefix + f"{char[0]}{line}{char[1]}"
    bottom = " " * len(prefix) + f"{char[3]}{line}{char[4]}"
    print(top)
    for i in range(h):
        l = f"{char[2]}{''.join([next(ic) for i in range(w)])}{char[2]}"
        if prefix != "":
            l = str(i * w).rjust(len(prefix)) + l
        print(l)
    print(bottom)


def heatmap_matrix_data(lines):
    high, width = len(lines), len(lines[0])
    line = width * char[-1]
    top = f"{char[0]}{line}{char[1]}"
    bottom = f"{char[3]}{line}{char[4]}"
    print(top)
    for l in lines:
        print(f"{char[2]}{''.join(l)}{char[2]}")
    print(bottom)


if __name__ == "__main__":
    import itertools

    data = []
    gen = itertools.cycle(ansi_heatmap_color)
    for i in range(14):
        data.append([ansi_color(next(gen), block_char_v[-1]) for j in range(64)])

    heatmap_matrix_data(data)

    heatmap_matrix_width_high(list(range(1000)), 64, 20, "Start Page")
