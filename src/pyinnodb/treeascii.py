import math
import itertools


def hello() -> str:
    return "Hello from treeascii!"


uni_h, uni_v, uni_tl, uni_tr, uni_br, uni_bl, uni_invt, uni_t, nl = (
    "\u2500\u2502\u256d\u256e\u256f\u2570\u2534\u252c\n"
)
uni_ten = "\u253c"
stripped = uni_h + uni_tl + uni_tr + " "


class TextBlock:
    def __init__(self, text: str):
        self.text = text
        self.lines = self._padded_lines(0, 0)

    def _find_span(self, s):
        n = len(s)
        r = len(s.rstrip(stripped))
        l = n - len(s.lstrip(stripped))
        return l, (l + r) // 2, r

    def _padded_lines(self, pad_l, pad_r):
        r, l = 0, math.inf
        for line in self.text.splitlines():
            if line.isspace():
                continue
            r = max(r, len(line.rstrip()))
            l = min(l, len(line) - len(line.lstrip()))
        extra_l = max(pad_l - l, 0)
        trim_l = max(l - pad_l, 0)
        prefix = " " * extra_l
        return [
            prefix + line[trim_l:].rstrip().ljust(r + pad_r)
            for line in self.text.splitlines()
        ]

    def build_top(self, kind=0) -> str:
        top_line = uni_h * self.size[1]
        l, mid, r = self.loc
        if kind == 0:
            l = mid
            top_line = l * " " + top_line[l:]
            top_line = top_line[:mid] + uni_tl + top_line[mid + 1 :]
        elif kind == 1:
            r = mid
            top_line = top_line[:mid] + uni_tr + " " * (self.size[1] - r - 1)
        elif kind == 2:
            top_line = top_line[:mid] + uni_t + top_line[mid + 1 :]
        elif kind == 3: # only one child
            return ' ' * self.size[1]
        return top_line

    @property
    def loc(self):
        return self._find_span(self.text.split("\n")[0])

    @property
    def size(self):
        return len(self.lines), len(self.lines[0])

    def pad(self, l, r):
        lines = self._padded_lines(l, r)
        self.lines = lines
        self.text = "\n".join(lines)

    def add_line(self, num):
        width = self.size[1]
        for i in range(num):
            self.lines.append(" " * width)
        self.text = "\n".join(self.lines)


def hjoin(blocks: list[TextBlock], sep=1) -> str:
    lines = []
    for ls in itertools.zip_longest(
        *[b.lines for b in blocks], fillvalue=" " * blocks[0].size[1]
    ):
        lines.append(" ".join(ls))
    return "\n".join(lines)


class TreeNode:
    def __init__(self, text: str, children=None):
        if children is None:
            children = []
        self.text = str(text)
        self.children = children

    @property
    def is_leaf(self) -> bool:
        return len(self.children) == 0

    def build_block(self, ellip_leaf=False, ellip_all=False) -> TextBlock:
        if self.is_leaf:
            return TextBlock(self.text)

        blocks = []
        ellip_leaf_cnt = 0
        for i, c in enumerate(self.children):
            if ellip_leaf and len(self.children) > 3:
                if i != 0 and i != len(self.children) - 1:
                    if c.is_leaf or ellip_all:
                        ellip_leaf_cnt += 1
                    else:
                        if ellip_leaf_cnt != 0:
                            blocks.append(TextBlock(f"({ellip_leaf_cnt} ellip)"))
                        blocks.append(c.build_block(ellip_leaf=ellip_leaf, ellip_all=ellip_all))
                        ellip_leaf_cnt = 0
                    continue
                if i == len(self.children) - 1:
                    if ellip_leaf_cnt != 0:
                        blocks.append(TextBlock(f"({ellip_leaf_cnt} ellip)"))
            blocks.append(c.build_block(ellip_leaf=ellip_leaf, ellip_all=ellip_all))

        max_lines = max(block.size[0] for block in blocks)
        tops = []
        if len(blocks) == 1:
            tops.append(blocks[0].build_top(kind=3))
        else:
            for i in range(len(blocks)):
                blocks[i].add_line(max_lines - blocks[i].size[0])
                if i == 0:
                    tops.append(blocks[i].build_top(kind=0))
                elif i == len(blocks) - 1:
                    tops.append(blocks[i].build_top(kind=1))
                else:
                    tops.append(blocks[i].build_top(kind=2))

        top_line = uni_h.join(tops)
        joined = TextBlock(hjoin(blocks))
        parent_text = (joined.size[1] - len(self.text)) // 2 * " " + self.text
        parent_text = TextBlock(parent_text.ljust(joined.size[1], " "))
        l, m, r = parent_text.loc
        p_mid = m
        r_text = uni_invt if top_line[p_mid] == uni_h else uni_ten
        r_text = uni_v if len(blocks) == 1 else r_text
        top_line = top_line[:p_mid] + r_text + top_line[p_mid + 1 :]
        return TextBlock("\n".join([parent_text.text, top_line, joined.text]))
