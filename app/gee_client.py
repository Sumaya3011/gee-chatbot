import os
import json
import ee
from google.oauth2 import service_account
from typing import Dict, Any

YEARS = [2020, 2021, 2022, 2023, 2024]
CLASS_PALETTE = [
    '419bdf','397d49','88b053','7a87c6','e49635',
    'dfc35a','c4281b','a59b8f','b39fe1'
]
CHANGE_COLOR = 'ff00ff'
RECT_BOUNDS = [54.16, 24.29, 54.74, 24.61]  # Abu Dhabi block


def initialize_ee_from_env():
    """Initialize Earth Engine using service account JSON in env var."""
    if ee.data._initialized:
        return

    key_json = os.environ.get("GEE_SERVICE_ACCOUNT_KEY")
    if not key_json:
        raise RuntimeError("GEE_SERVICE_ACCOUNT_KEY not set in environment.")

    key_dict = json.loads(key_json)
    credentials = service_account.Credentials.from_service_account_info(
        key_dict,
        scopes=['https://www.googleapis.com/auth/earthengine']
    )
    ee.Initialize(credentials)


def yearly_dw_label(year: int, roi: ee.Geometry) -> ee.Image:
    start = ee.Date.fromYMD(year, 1, 1)
    end = start.advance(1, 'year')
    coll = (ee.ImageCollection('GOOGLE/DYNAMICWORLD/V1')
            .filterBounds(roi)
            .filterDate(start, end)
            .select('label'))
    img = coll.mode().clip(roi).unmask(0).set('system:time_start', start.millis())
    return img


def yearly_s2_rgb(year: int, roi: ee.Geometry) -> ee.Image:
    start = ee.Date.fromYMD(year, 1, 1)
    coll = (ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
            .filterBounds(roi)
            .filterDate(start, start.advance(1, 'year'))
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30)))
    median = coll.median().clip(roi)
    vis = median.visualize(bands=['B4', 'B3', 'B2'], min=0, max=3000)
    return vis


def build_vis_collection_range(y_start: int, y_end: int, roi: ee.Geometry) -> ee.ImageCollection:
    years_list = ee.List.sequence(y_start, y_end)

    def map_year(y):
        y = ee.Number(y)
        img = yearly_dw_label(y, roi).visualize(min=0, max=8, palette=CLASS_PALETTE)
        return img.set('system:time_start', ee.Date.fromYMD(y, 1, 1).millis())

    return ee.ImageCollection(years_list.map(map_year))


def make_change_layer(yA: int, yB: int, roi: ee.Geometry) -> ee.Image:
    imgA = yearly_dw_label(yA, roi)
    imgB = yearly_dw_label(yB, roi)
    ch = imgA.neq(imgB).selfMask().clip(roi)
    vis = ch.visualize(palette=[CHANGE_COLOR])
    return vis


def get_roi_from_params(params: Dict[str, Any]) -> ee.Geometry:
    bounds = params.get("bounds")
    if bounds and isinstance(bounds, (list, tuple)) and len(bounds) == 4:
        return ee.Geometry.Rectangle(bounds, None, False)
    return ee.Geometry.Rectangle(RECT_BOUNDS, None, False)


def image_thumbnail_url(image: ee.Image, region: ee.Geometry, dims: int = 768) -> str:
    params = {
        'region': region.bounds().getInfo()['coordinates'],
        'dimensions': dims
    }
    url = image.getThumbURL(params)
    return url


def collection_video_thumb_url(coll: ee.ImageCollection, region: ee.Geometry,
                               fps: int = 1, dims: int = 768) -> str:
    params = {
        'region': region.bounds().getInfo()['coordinates'],
        'framesPerSecond': fps,
        'dimensions': dims
    }
    url = coll.getVideoThumbURL(params)
    return url


def run_gee_task(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    params example:
    {
      "yearA": 2020,
      "yearB": 2024,
      "bounds": [54.16, 24.29, 54.74, 24.61],
      "video": true
    }
    """
    initialize_ee_from_env()

    yearA = int(params.get("yearA", YEARS[0]))
    yearB = int(params.get("yearB", YEARS[-1]))
    if yearA > yearB:
        yearA, yearB = yearB, yearA

    roi = get_roi_from_params(params)
    thumb_dims = int(params.get("thumb_dims", 768))
    produce_video = bool(params.get("video", False))
    video_fps = int(params.get("video_fps", 1))

    dw_vis_A = yearly_dw_label(yearA, roi).visualize(min=0, max=8, palette=CLASS_PALETTE)
    dw_vis_B = yearly_dw_label(yearB, roi).visualize(min=0, max=8, palette=CLASS_PALETTE)
    s2_vis_A = yearly_s2_rgb(yearA, roi)
    s2_vis_B = yearly_s2_rgb(yearB, roi)
    change_vis = make_change_layer(yearA, yearB, roi)

    urls = {
        "dw_A_thumb": image_thumbnail_url(dw_vis_A, roi, dims=thumb_dims),
        "dw_B_thumb": image_thumbnail_url(dw_vis_B, roi, dims=thumb_dims),
        "s2_A_thumb": image_thumbnail_url(s2_vis_A, roi, dims=thumb_dims),
        "s2_B_thumb": image_thumbnail_url(s2_vis_B, roi, dims=thumb_dims),
        "change_thumb": image_thumbnail_url(change_vis, roi, dims=thumb_dims),
    }

    # simple DW histogram for yearA
    try:
        freqA = yearly_dw_label(yearA, roi).reduceRegion(
            reducer=ee.Reducer.frequencyHistogram(),
            geometry=roi,
            scale=30,
            maxPixels=1e9
        ).getInfo()
    except Exception as e:
        freqA = {"error": str(e)}

    video_url = None
    if produce_video:
        vis_coll = build_vis_collection_range(yearA, yearB, roi)
        try:
            video_url = collection_video_thumb_url(vis_coll, roi, fps=video_fps, dims=thumb_dims)
        except Exception:
            video_url = None

    summary_text = f"Dynamic World for years {yearA} → {yearB} over the Abu Dhabi city block."

    return {
        "summary": summary_text,
        "yearA": yearA,
        "yearB": yearB,
        "urls": urls,
        "histogram_yearA": freqA,
        "video_url": video_url,
    }
