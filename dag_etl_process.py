import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np

fig, ax = plt.subplots(figsize=(18, 10))
ax.set_xlim(-0.5, 18.5)
ax.set_ylim(-1.0, 8.2)
ax.set_aspect('equal')
ax.axis('off')

fig.patch.set_facecolor('#F7F9FC')
ax.set_facecolor('#F7F9FC')

# --- Фонові зони етапів ---
# Extract: x = -0.2 .. 4.4  (ширина 4.6)
# Transform: x = 4.7 .. 13.3 (ширина 8.6)
# Load: x = 13.6 .. 18.2   (ширина 4.6)
ZONE_Y, ZONE_H = 0.6, 6.4
zone_configs = [
    (-0.2, ZONE_Y, 4.6, ZONE_H, '#EAF4FB', '#2F75B5', 'EXTRACT\n(витягування)'),
    ( 4.7, ZONE_Y, 8.6, ZONE_H, '#EDF7E8', '#548235', 'TRANSFORM\n(трансформація)'),
    (13.6, ZONE_Y, 4.6, ZONE_H, '#FEF0E7', '#C55A11', 'LOAD\n(завантаження)'),
]
for x, y, w, h, fc, ec, label in zone_configs:
    zone = FancyBboxPatch((x, y), w, h,
                          boxstyle="round,pad=0.15",
                          linewidth=2, edgecolor=ec,
                          facecolor=fc, zorder=0, alpha=0.6)
    ax.add_patch(zone)
    ax.text(x + w / 2, y + h + 0.25, label,
            fontsize=22, fontweight='bold', ha='center', va='bottom',
            color=ec, zorder=5)

# --- Вузли ---
# Extract (x≈2.0):  зона -0.2..4.4 → центр 2.1, вузол ±1.0 → 1.1..3.1 ✓
# M1     (x≈6.5):  зона 4.7..13.3 ✓
# T1-T3  (x≈9.5):  зона 4.7..13.3, вузол 8.5..10.5 ✓
# A1     (x≈12.0): зона 4.7..13.3, вузол 11.0..13.0 ✓
# Load   (x≈15.9): зона 13.6..18.2, вузол 14.9..16.9 ✓
node_defs = {
    # Extract
    'E1': (2.1, 5.5, '#D0E8F7', '#2F75B5', '1. Витягування\nданих з БД'),
    'E2': (2.1, 3.5, '#D0E8F7', '#2F75B5', '2. Витягування\nданих з файлів'),
    'E3': (2.1, 1.5, '#B8D9F0', '#1A5276', '3. Джерела API\nта стримінг'),
    # Merge (Transform zone)
    'M1': (6.5, 3.5, '#C8E6C9', '#2E7D32', '4. Об\'єднання\nсирих даних'),
    # Transform
    'T1': (9.5, 5.5, '#C5E8BC', '#548235', '5. Очищення\nданих'),
    'T2': (9.5, 3.5, '#C5E8BC', '#548235', '6. Нормалізація\nданих'),
    'T3': (9.5, 1.5, '#C5E8BC', '#548235', '7. Збагачення\nданих'),
    # Aggregate (Transform zone)
    'A1': (12.0, 3.5, '#A8D5A2', '#1B5E20', '8. Агрегація\nрезультатів'),
    # Load
    'L1': (15.9, 5.5, '#FBCFB0', '#C55A11', '9. Сховище\nданих (DWH)'),
    'L2': (15.9, 3.5, '#FBCFB0', '#C55A11', '10. Файл /\nзвіт (CSV)'),
    'L3': (15.9, 1.5, '#FBCFB0', '#C55A11', '11. API /\nдашборд'),
}

BOX_W, BOX_H = 2.0, 1.1

def draw_node(ax, cx, cy, fc, ec, label):
    x0, y0 = cx - BOX_W / 2, cy - BOX_H / 2
    box = FancyBboxPatch((x0, y0), BOX_W, BOX_H,
                         boxstyle="round,pad=0.12",
                         linewidth=2.2, edgecolor=ec,
                         facecolor=fc, zorder=3)
    ax.add_patch(box)
    # Тонка тінь
    shadow = FancyBboxPatch((x0 + 0.07, y0 - 0.07), BOX_W, BOX_H,
                            boxstyle="round,pad=0.12",
                            linewidth=0, facecolor='#BBBBBB',
                            alpha=0.35, zorder=2)
    ax.add_patch(shadow)
    ax.text(cx, cy, label, fontsize=13, ha='center', va='center',
            fontweight='bold', color='#1A1A1A', zorder=4,
            multialignment='center')

for key, (cx, cy, fc, ec, label) in node_defs.items():
    draw_node(ax, cx, cy, fc, ec, label)

# --- Стрілки ---
def arrow(ax, x1, y1, x2, y2, color='#444444', rad=0.0):
    style = f"arc3,rad={rad}"
    arr = FancyArrowPatch(
        (x1, y1), (x2, y2),
        connectionstyle=style,
        arrowstyle='-|>', mutation_scale=18,
        linewidth=1.8, color=color, zorder=1
    )
    ax.add_patch(arr)

def node_right(key):
    cx, cy = node_defs[key][0], node_defs[key][1]
    return cx + BOX_W / 2, cy

def node_left(key):
    cx, cy = node_defs[key][0], node_defs[key][1]
    return cx - BOX_W / 2, cy

# Extract → Merge
for src in ('E1', 'E2', 'E3'):
    sx, sy = node_right(src)
    tx, ty = node_left('M1')
    rad = 0.15 if src == 'E1' else (-0.15 if src == 'E3' else 0.0)
    arrow(ax, sx, sy, tx, ty, color='#2F75B5', rad=rad)

# Merge → Transform
for tgt in ('T1', 'T2', 'T3'):
    sx, sy = node_right('M1')
    tx, ty = node_left(tgt)
    rad = 0.15 if tgt == 'T1' else (-0.15 if tgt == 'T3' else 0.0)
    arrow(ax, sx, sy, tx, ty, color='#548235', rad=rad)

# Transform → Aggregate
for src in ('T1', 'T2', 'T3'):
    sx, sy = node_right(src)
    tx, ty = node_left('A1')
    rad = 0.15 if src == 'T1' else (-0.15 if src == 'T3' else 0.0)
    arrow(ax, sx, sy, tx, ty, color='#548235', rad=rad)

# Aggregate → Load
for tgt in ('L1', 'L2', 'L3'):
    sx, sy = node_right('A1')
    tx, ty = node_left(tgt)
    rad = 0.15 if tgt == 'L1' else (-0.15 if tgt == 'L3' else 0.0)
    arrow(ax, sx, sy, tx, ty, color='#C55A11', rad=rad)

# --- Легенда ---
legend_items = [
    mpatches.Patch(facecolor='#D0E8F7', edgecolor='#2F75B5', linewidth=1.5, label='Extract — витягування даних'),
    mpatches.Patch(facecolor='#C5E8BC', edgecolor='#548235', linewidth=1.5, label='Transform — трансформація'),
    mpatches.Patch(facecolor='#FBCFB0', edgecolor='#C55A11', linewidth=1.5, label='Load — завантаження'),
]
ax.legend(handles=legend_items, loc='lower center',
          ncol=3, fontsize=12, framealpha=0.85,
          edgecolor='#CCCCCC', bbox_to_anchor=(0.5, -0.08))

plt.tight_layout()
plt.savefig("dag_etl_process.png", dpi=300, bbox_inches="tight",
            facecolor=fig.get_facecolor())
print("Збережено: dag_etl_process.png")
