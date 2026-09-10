# Polign UI assets

The transfer console follows the sibling `polign_web` site's colors, typography,
header, rounded controls, and flower hero treatment. `control/web/bloom.jpg` and
`control/web/favicon.svg` are copied from that site's `assets/img/bloom.jpg` and
favicon; the header uses its SVG Polign mark.

Google Sans Flex is served locally from `control/web/polign-sans.woff2`, with its
SIL Open Font License in `control/web/FONT-LICENSE.txt`. The font is from Google
Fonts; the license is from `googlefonts/googlesans-flex`. No external font or
image requests are needed. These assets are embedded in the Go binary.
