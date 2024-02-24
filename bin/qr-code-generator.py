import qrcode
from qrcode.image.styledpil import StyledPilImage
from qrcode.image.styles.moduledrawers.pil import RoundedModuleDrawer
from qrcode.image.svg import SvgPathImage 
from qrcode.image.styledpil import StyledPilImage
from qrcode.image.styles.colormasks import RadialGradiantColorMask
from PIL import Image
from PIL import ImageFilter

# from qrcode.image.styles.moduledrawers.pil import RoundedModuleDrawer
# from qrcode.image.styles.colormasks import RadialGradiantColorMask

qr = qrcode.QRCode(
    version=None,
    error_correction=qrcode.constants.ERROR_CORRECT_H,
    box_size=15,
    border=1,
)
# factory = SvgCircleDrawer
factory = StyledPilImage
# drawer=RoundedModuleDrawer
drawer=RadialGradiantColorMask
# factory = StyledPilImage
qr.add_data('https://grc.iit.edu/research/projects/hermes')
qr.make(fit=True)

# img = qr.make_image(image_factory=StyledPilImage, embeded_image_path="/home/neeraj/Downloads/GRC-Logo-Square-Black.png")
# img = qr.make_image(embeded_image_path="/home/neeraj/Downloads/GRC-Logo-Square-Black.png", back_color=(20,157,204), fill_color=(255, 255, 255), image_factory=factory)
# img_1 = qr.make_image(image_factory=StyledPilImage, module_drawer=RoundedModuleDrawer())
img = qr.make_image(embeded_image_path="/home/lukemartinlogan/Downloads/github-icon.png",
                    image_factory=factory,
                    embeded_image_resample=Image.LANCZOS,
                    color_mask=RadialGradiantColorMask(back_color=(255, 255, 255), edge_color=(20, 157, 204), center_color= (0,0,0) ),
                    # module_drawer=drawer(),
                    fill_color="#149dcc",
                    back_color="#ffffff",
                    )
# img = qr.make_image(embeded_image_path="/home/neeraj/Downloads/GRC-Logo-Square-Black.png", fill_color=(), )
img.save("hermes-git.png")
