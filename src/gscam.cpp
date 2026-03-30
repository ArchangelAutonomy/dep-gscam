// Copyright 2022 Jonathan Bohren, Clyde McQueen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <iostream>
#include <string>

extern "C" {
#include "gst/gst.h"
#include "gst/app/gstappsink.h"
}

#include "image_transport/image_transport.hpp"
#include "camera_info_manager/camera_info_manager.hpp"

#include "sensor_msgs/msg/image.hpp"
#include "sensor_msgs/msg/compressed_image.hpp"
#include "sensor_msgs/msg/camera_info.hpp"
#include "sensor_msgs/image_encodings.hpp"
#include "std_msgs/msg/header.hpp"

#include "gscam/gscam.hpp"

namespace gscam
{

GSCam::GSCam(const rclcpp::NodeOptions & options)
: rclcpp::Node("gscam_publisher", options),
  gsconfig_(""),
  udp_streaming_enabled_(false),
  pipeline_(NULL),
  sink_(NULL),
  camera_info_manager_(this, "camera", ""),
  stop_signal_(false),
  diag_updater_(this)
#ifdef HAVE_GST_RTSP_SERVER
  , rtsp_streaming_enabled_(false),
  rtsp_port_(8554),
  rtsp_appsink_(NULL),
  rtsp_appsrc_(NULL),
  rtsp_server_(NULL),
  rtsp_context_(NULL),
  rtsp_main_loop_(NULL)
#endif
{
  pipeline_thread_ = std::thread(
    [this]()
    {
      run();
    });
}

GSCam::~GSCam()
{
  stop_signal_ = true;
  pipeline_thread_.join();
}

bool GSCam::configure()
{
  // Get gstreamer configuration
  // (either from environment variable or ROS param)
  bool gsconfig_rosparam_defined = false;
  char * gsconfig_env = NULL;

  const auto gsconfig_rosparam = declare_parameter("gscam_config", "");
  gsconfig_rosparam_defined = !gsconfig_rosparam.empty();
  gsconfig_env = getenv("GSCAM_CONFIG");

  if (!gsconfig_env && !gsconfig_rosparam_defined) {
    RCLCPP_FATAL(
      get_logger(),
      "Problem getting GSCAM_CONFIG environment variable and "
      "'gscam_config' rosparam is not set. This is needed to set up a gstreamer pipeline.");
    return false;
  } else if (gsconfig_env && gsconfig_rosparam_defined) {
    RCLCPP_FATAL(
      get_logger(),
      "Both GSCAM_CONFIG environment variable and 'gscam_config' rosparam are set. "
      "Please only define one.");
    return false;
  } else if (gsconfig_env) {
    gsconfig_ = gsconfig_env;
    RCLCPP_INFO_STREAM(
      get_logger(),
      "Using gstreamer config from env: \"" << gsconfig_env << "\"");
  } else if (gsconfig_rosparam_defined) {
    gsconfig_ = gsconfig_rosparam;
    RCLCPP_INFO_STREAM(
      get_logger(),
      "Using gstreamer config from rosparam: \"" << gsconfig_rosparam << "\"");
  }

  // Get additional gscam configuration
  sync_sink_ = declare_parameter("sync_sink", true);
  preroll_ = declare_parameter("preroll", false);
  use_gst_timestamps_ = declare_parameter("use_gst_timestamps", false);

  reopen_on_eof_ = declare_parameter("reopen_on_eof", false);

  // Get the camera parameters file
  camera_info_url_ = declare_parameter("camera_info_url", "");
  camera_name_ = declare_parameter("camera_name", "");

  diag_min_freq_ = declare_parameter("camera_expected_min_fps", 13.0);
  diag_max_freq_ = declare_parameter("camera_expected_max_fps", 17.0);
  diag_updater_.setHardwareID(declare_parameter("hardware_id", "unknown"));

  // Get the image encoding
  image_encoding_ =
    declare_parameter("image_encoding", std::string(sensor_msgs::image_encodings::RGB8));
  if (image_encoding_ != sensor_msgs::image_encodings::RGB8 &&
    image_encoding_ != sensor_msgs::image_encodings::MONO8 &&
    image_encoding_ != sensor_msgs::image_encodings::YUV422 &&
    image_encoding_ != "jpeg")
  {
    RCLCPP_FATAL_STREAM(get_logger(), "Unsupported image encoding: " + image_encoding_);
  }

  camera_info_manager_.setCameraName(camera_name_);

  if (camera_info_manager_.validateURL(camera_info_url_)) {
    camera_info_manager_.loadCameraInfo(camera_info_url_);
    RCLCPP_INFO_STREAM(get_logger(), "Loaded camera calibration from " << camera_info_url_);
  } else {
    RCLCPP_WARN_STREAM(
      get_logger(),
      "Camera info at: " << camera_info_url_ << " not found. Using an uncalibrated config.");
  }

  // Get TF Frame
  frame_id_ = declare_parameter("frame_id", "camera_frame");
  if (frame_id_ == "camera_frame") {
    RCLCPP_WARN_STREAM(
      get_logger(),
      "No camera frame_id set, using frame \"" << frame_id_ << "\".");
  }

  use_sensor_data_qos_ = declare_parameter("use_sensor_data_qos", false);

  // Get acquisition and gst timestamp offset
  auto param_desc = rcl_interfaces::msg::ParameterDescriptor{};
  param_desc.description = "Time offset [seconds] between gst_timestamps and camera shutter.";
  acquisition_offset_ = declare_parameter<int64_t>("acquisition_offset", 0, param_desc);

  // UDP streaming: when udp_stream_config is non-empty, a tee is inserted so the camera
  // source feeds both a ROS appsink and a hardware-encoded UDP stream simultaneously.
  // - gscam_config       : source pipeline up to (but not including) the tee
  // - ros_branch_config  : pipeline from the tee output to just before appsink
  //                        (e.g. "nvvidconv flip-method=0 ! video/x-raw,format=BGRx ! videoconvert")
  // - udp_stream_config  : full pipeline from the tee output to udpsink
  //                        (e.g. "nvv4l2h264enc insert-sps-pps=true ! h264parse ! rtph264pay pt=96
  //                               ! udpsink host=192.168.1.100 port=5000")
  udp_stream_config_ = declare_parameter("udp_stream_config", "");
  ros_branch_config_ = declare_parameter("ros_branch_config", "");
  udp_streaming_enabled_ = !udp_stream_config_.empty();

  if (udp_streaming_enabled_ && ros_branch_config_.empty()) {
    RCLCPP_FATAL(
      get_logger(),
      "'ros_branch_config' must be set when 'udp_stream_config' is set. "
      "It is the pipeline segment from the tee to the appsink (ROS branch).");
    return false;
  }

  if (udp_streaming_enabled_) {
    RCLCPP_INFO_STREAM(get_logger(), "UDP streaming enabled.");
    RCLCPP_INFO_STREAM(get_logger(), "  ros_branch_config: " << ros_branch_config_);
    RCLCPP_INFO_STREAM(get_logger(), "  udp_stream_config: " << udp_stream_config_);
  }

#ifdef HAVE_GST_RTSP_SERVER
  // RTSP streaming parameters.
  // - rtsp_encode_config : tee branch from source to encoded bitstream before the internal
  //                        appsink (e.g. "nvv4l2h264enc insert-sps-pps=true ! h264parse")
  // - rtsp_pay_config    : RTP packetizer placed in the RTSP factory pipeline
  //                        (e.g. "rtph264pay name=pay0 pt=96")
  // - rtsp_port          : TCP port the RTSP server listens on (default 8554)
  // - rtsp_mount_point   : URL path where the stream is served (default "/stream")
  rtsp_encode_config_ = declare_parameter("rtsp_encode_config", "");
  rtsp_pay_config_ = declare_parameter("rtsp_pay_config", "rtph264pay name=pay0 pt=96");
  rtsp_port_ = declare_parameter("rtsp_port", 8554);
  rtsp_mount_point_ = declare_parameter("rtsp_mount_point", "/stream");
  rtsp_streaming_enabled_ = !rtsp_encode_config_.empty();

  if (rtsp_streaming_enabled_) {
    RCLCPP_INFO_STREAM(get_logger(), "RTSP streaming enabled.");
    RCLCPP_INFO_STREAM(get_logger(), "  rtsp_encode_config: " << rtsp_encode_config_);
    RCLCPP_INFO_STREAM(get_logger(), "  rtsp_pay_config:    " << rtsp_pay_config_);
    RCLCPP_INFO_STREAM(
      get_logger(), "  endpoint: rtsp://localhost:" << rtsp_port_ << rtsp_mount_point_);
  }
#else
  // Declare params so they exist even when RTSP is compiled out (avoids unknown-param errors).
  {
    const auto rtsp_encode = declare_parameter("rtsp_encode_config", "");
    declare_parameter("rtsp_pay_config", "rtph264pay name=pay0 pt=96");
    declare_parameter("rtsp_port", 8554);
    declare_parameter("rtsp_mount_point", "/stream");
    if (!rtsp_encode.empty()) {
      RCLCPP_WARN(
        get_logger(),
        "'rtsp_encode_config' is set but this build was compiled without gst-rtsp-server support. "
        "Install libgstreamer-rtsp-server-1.0-dev and rebuild to enable RTSP.");
    }
  }
#endif

  // A tee is needed whenever any secondary output branch is active.
  // ros_branch_config is required in that case.
  const bool need_tee = udp_streaming_enabled_
#ifdef HAVE_GST_RTSP_SERVER
    || rtsp_streaming_enabled_
#endif
  ;
  if (need_tee && ros_branch_config_.empty()) {
    RCLCPP_FATAL(
      get_logger(),
      "'ros_branch_config' must be set when any secondary stream (UDP or RTSP) is enabled. "
      "It is the pipeline segment from the tee to the ROS appsink.");
    return false;
  }

  return true;
}

bool GSCam::init_stream()
{
  if (!gst_is_initialized()) {
    // Initialize gstreamer pipeline
    RCLCPP_DEBUG_STREAM(get_logger(), "Initializing gstreamer...");
    gst_init(0, 0);
  }

  RCLCPP_DEBUG_STREAM(get_logger(), "Gstreamer Version: " << gst_version_string() );

  GError * error = 0;  // Assignment to zero is a gst requirement

  const bool need_tee = udp_streaming_enabled_
#ifdef HAVE_GST_RTSP_SERVER
    || rtsp_streaming_enabled_
#endif
  ;

  if (need_tee) {
    // Build a single pipeline with a tee so the camera source is opened only once.
    // Each active output gets its own queue-decoupled branch.
    //
    // Full pipeline structure:
    //   {gscam_config} ! tee name=_xnaut_tee_
    //     _xnaut_tee_. ! queue ! {ros_branch_config} ! appsink name=_xnaut_appsink_
    //     _xnaut_tee_. ! queue ! {udp_stream_config}                         (if UDP)
    //     _xnaut_tee_. ! queue ! {rtsp_encode_config} ! appsink name=_xnaut_rtsp_appsink_  (if RTSP)
    std::string full_pipeline =
      gsconfig_ +
      " ! tee name=_xnaut_tee_"
      " _xnaut_tee_. ! queue ! " + ros_branch_config_ +
      " ! appsink name=_xnaut_appsink_";

    if (udp_streaming_enabled_) {
      full_pipeline += " _xnaut_tee_. ! queue ! " + udp_stream_config_;
    }

#ifdef HAVE_GST_RTSP_SERVER
    if (rtsp_streaming_enabled_) {
      full_pipeline +=
        " _xnaut_tee_. ! queue ! " + rtsp_encode_config_ +
        " ! appsink name=_xnaut_rtsp_appsink_ emit-signals=false";
    }
#endif

    RCLCPP_INFO_STREAM(get_logger(), "Full tee pipeline: " << full_pipeline);

    pipeline_ = gst_parse_launch(full_pipeline.c_str(), &error);
    if (pipeline_ == NULL) {
      RCLCPP_FATAL_STREAM(get_logger(), error->message);
      return false;
    }

    // gst_bin_get_by_name adds an extra ref; each is released in cleanup_stream()
    sink_ = gst_bin_get_by_name(GST_BIN(pipeline_), "_xnaut_appsink_");
    if (!sink_) {
      RCLCPP_FATAL(get_logger(), "Failed to find ROS appsink element in tee pipeline");
      gst_object_unref(pipeline_);
      pipeline_ = NULL;
      return false;
    }

    // Set caps and sync on the ROS appsink
    GstCaps * caps = NULL;
    // http://gstreamer.freedesktop.org/data/doc/gstreamer/head/pwg/html/section-types-definitions.html
    if (image_encoding_ == sensor_msgs::image_encodings::RGB8) {
      caps = gst_caps_new_simple("video/x-raw", "format", G_TYPE_STRING, "RGB", NULL);
    } else if (image_encoding_ == sensor_msgs::image_encodings::MONO8) {
      caps = gst_caps_new_simple("video/x-raw", "format", G_TYPE_STRING, "GRAY8", NULL);
    } else if (image_encoding_ == sensor_msgs::image_encodings::YUV422) {
      caps = gst_caps_new_simple("video/x-raw", "format", G_TYPE_STRING, "UYVY", NULL);
    } else if (image_encoding_ == "jpeg") {
      caps = gst_caps_new_simple("image/jpeg", NULL, NULL);
    }
    if (caps) {
      gst_app_sink_set_caps(GST_APP_SINK(sink_), caps);
      gst_caps_unref(caps);
    }
    gst_base_sink_set_sync(GST_BASE_SINK(sink_), sync_sink_ ? TRUE : FALSE);

#ifdef HAVE_GST_RTSP_SERVER
    if (rtsp_streaming_enabled_) {
      rtsp_appsink_ = gst_bin_get_by_name(GST_BIN(pipeline_), "_xnaut_rtsp_appsink_");
      if (!rtsp_appsink_) {
        RCLCPP_FATAL(get_logger(), "Failed to find RTSP appsink element in tee pipeline");
        gst_object_unref(sink_);
        sink_ = NULL;
        gst_object_unref(pipeline_);
        pipeline_ = NULL;
        return false;
      }
      // Drop frames rather than queue them when no RTSP client is connected yet.
      gst_app_sink_set_max_buffers(GST_APP_SINK(rtsp_appsink_), 2);
      gst_app_sink_set_drop(GST_APP_SINK(rtsp_appsink_), TRUE);
      gst_base_sink_set_sync(GST_BASE_SINK(rtsp_appsink_), FALSE);

      // Callback drives the appsink → appsrc bridge.
      GstAppSinkCallbacks rtsp_cb = {};
      rtsp_cb.new_sample = &GSCam::on_rtsp_new_sample;
      gst_app_sink_set_callbacks(GST_APP_SINK(rtsp_appsink_), &rtsp_cb, this, NULL);

      setup_rtsp_server();
    }
#endif
  } else {
    // Existing behavior: parse the user's pipeline and programmatically append appsink
    pipeline_ = gst_parse_launch(gsconfig_.c_str(), &error);
    if (pipeline_ == NULL) {
      RCLCPP_FATAL_STREAM(get_logger(), error->message);
      return false;
    }

    // Create RGB sink
    sink_ = gst_element_factory_make("appsink", NULL);
    GstCaps * caps = gst_app_sink_get_caps(GST_APP_SINK(sink_));

    // http://gstreamer.freedesktop.org/data/doc/gstreamer/head/pwg/html/section-types-definitions.html
    if (image_encoding_ == sensor_msgs::image_encodings::RGB8) {
      caps = gst_caps_new_simple(
        "video/x-raw",
        "format", G_TYPE_STRING, "RGB",
        NULL);
    } else if (image_encoding_ == sensor_msgs::image_encodings::MONO8) {
      caps = gst_caps_new_simple(
        "video/x-raw",
        "format", G_TYPE_STRING, "GRAY8",
        NULL);
    } else if (image_encoding_ == sensor_msgs::image_encodings::YUV422) {
      caps = gst_caps_new_simple(
        "video/x-raw",
        "format", G_TYPE_STRING, "UYVY",
        NULL);
    } else if (image_encoding_ == "jpeg") {
      caps = gst_caps_new_simple("image/jpeg", NULL, NULL);
    }

    gst_app_sink_set_caps(GST_APP_SINK(sink_), caps);
    gst_caps_unref(caps);

    // Set whether the sink should sync
    // Sometimes setting this to true can cause a large number of frames to be
    // dropped
    gst_base_sink_set_sync(
      GST_BASE_SINK(sink_),
      (sync_sink_) ? TRUE : FALSE);

    if (GST_IS_PIPELINE(pipeline_)) {
      GstPad * outpad = gst_bin_find_unlinked_pad(GST_BIN(pipeline_), GST_PAD_SRC);
      g_assert(outpad);

      GstElement * outelement = gst_pad_get_parent_element(outpad);
      g_assert(outelement);
      gst_object_unref(outpad);

      if (!gst_bin_add(GST_BIN(pipeline_), sink_)) {
        RCLCPP_FATAL(get_logger(), "gst_bin_add() failed");
        gst_object_unref(outelement);
        gst_object_unref(pipeline_);
        return false;
      }

      if (!gst_element_link(outelement, sink_)) {
        RCLCPP_FATAL(
          get_logger(), "GStreamer: cannot link outelement(\"%s\") -> sink\n",
          gst_element_get_name(outelement));
        gst_object_unref(outelement);
        gst_object_unref(pipeline_);
        return false;
      }

      gst_object_unref(outelement);
    } else {
      GstElement * launchpipe = pipeline_;
      pipeline_ = gst_pipeline_new(NULL);
      g_assert(pipeline_);

      gst_object_unparent(GST_OBJECT(launchpipe));

      gst_bin_add_many(GST_BIN(pipeline_), launchpipe, sink_, NULL);

      if (!gst_element_link(launchpipe, sink_)) {
        RCLCPP_FATAL(get_logger(), "GStreamer: cannot link launchpipe -> sink");
        gst_object_unref(pipeline_);
        return false;
      }
    }
  }

  // Calibration between ros::Time and gst timestamps
  GstClock * clock = gst_system_clock_obtain();
  GstClockTime ct = gst_clock_get_time(clock);
  gst_object_unref(clock);
  time_offset_ = now().nanoseconds() - GST_TIME_AS_NSECONDS(ct);
  RCLCPP_INFO(get_logger(), "Time offset: %.6f", rclcpp::Time(time_offset_).seconds());

  gst_element_set_state(pipeline_, GST_STATE_PAUSED);

  if (gst_element_get_state(pipeline_, NULL, NULL, -1) == GST_STATE_CHANGE_FAILURE) {
    RCLCPP_FATAL(get_logger(), "Failed to PAUSE stream, check your gstreamer configuration.");
    return false;
  } else {
    RCLCPP_DEBUG_STREAM(get_logger(), "Stream is PAUSED.");
  }

  // Create ROS camera interface
  const auto qos = use_sensor_data_qos_ ? rclcpp::SensorDataQoS() : rclcpp::QoS{1};
  if (image_encoding_ == "jpeg") {
    jpeg_pub_ =
      create_publisher<sensor_msgs::msg::CompressedImage>(
      "camera/image_raw/compressed", qos);
    cinfo_pub_ = create_publisher<sensor_msgs::msg::CameraInfo>(
      "camera/camera_info", qos);
  } else {
#ifdef USE_OLD_RMW_QOS_IMAGE_TRANSPORT
    camera_pub_ = image_transport::create_camera_publisher(
      this, "camera/image_raw", qos.get_rmw_qos_profile());
#else
    camera_pub_ = image_transport::create_camera_publisher(
      *this, "camera/image_raw", qos);
#endif
  }

  // Heartbeat
  heartbeat_pub_ = create_publisher<std_msgs::msg::Header>("~/heartbeat", rclcpp::QoS{1});
  heartbeat_timer_ = create_wall_timer(
    std::chrono::seconds(1),
    [this]() {
      std_msgs::msg::Header msg;
      msg.stamp = now();
      msg.frame_id = frame_id_;
      heartbeat_pub_->publish(msg);
    });

  // Diagnostics
  diagnostic_updater::FrequencyStatusParam freq_params{&diag_min_freq_, &diag_max_freq_};
  img_freq_diag_ = std::make_unique<diagnostic_updater::TopicDiagnostic>(
    "Image pub rate", diag_updater_, freq_params, diagnostic_updater::DefaultTimeStampStatusParam);

  return true;
}

void GSCam::publish_stream()
{
  RCLCPP_INFO_STREAM(get_logger(), "Publishing stream...");

  // Pre-roll camera if needed
  if (preroll_) {
    RCLCPP_DEBUG(get_logger(), "Performing preroll...");

    // The PAUSE, PLAY, PAUSE, PLAY cycle is to ensure proper pre-roll
    // I am told this is needed and am erring on the side of caution.
    gst_element_set_state(pipeline_, GST_STATE_PLAYING);
    if (gst_element_get_state(pipeline_, NULL, NULL, -1) == GST_STATE_CHANGE_FAILURE) {
      RCLCPP_ERROR(get_logger(), "Failed to PLAY during preroll.");
      return;
    } else {
      RCLCPP_DEBUG(get_logger(), "Stream is PLAYING in preroll.");
    }

    gst_element_set_state(pipeline_, GST_STATE_PAUSED);
    if (gst_element_get_state(pipeline_, NULL, NULL, -1) == GST_STATE_CHANGE_FAILURE) {
      RCLCPP_ERROR(get_logger(), "Failed to PAUSE.");
      return;
    } else {
      RCLCPP_INFO(get_logger(), "Stream is PAUSED in preroll.");
    }
  }

  if (gst_element_set_state(pipeline_, GST_STATE_PLAYING) == GST_STATE_CHANGE_FAILURE) {
    RCLCPP_ERROR(get_logger(), "Could not start stream!");
    return;
  }
  RCLCPP_INFO(get_logger(), "Started stream.");

  // Poll the data as fast a spossible
  while (!stop_signal_ && rclcpp::ok()) {
    // This should block until a new frame is awake, this way, we'll run at the
    // actual capture framerate of the device.
    // RCLCPP_DEBUG(get_logger(), "Getting data...");
    GstSample * sample = gst_app_sink_pull_sample(GST_APP_SINK(sink_));
    if (!sample) {
      RCLCPP_ERROR(get_logger(), "Could not get gstreamer sample.");
      break;
    }
    GstBuffer * buf = gst_sample_get_buffer(sample);
    GstMemory * memory = gst_buffer_get_memory(buf, 0);
    GstMapInfo info;

    gst_memory_map(memory, &info, GST_MAP_READ);
    gsize & buf_size = info.size;
    guint8 * & buf_data = info.data;

    GstClockTime bt = gst_element_get_base_time(pipeline_);
    // RCLCPP_INFO(
    //   get_logger(),
    //   "New buffer: timestamp %.6f %lu %lu %.3f",
    //   GST_TIME_AS_USECONDS(buf->timestamp + bt) / 1e6 + time_offset_,
    //   buf->timestamp, bt, time_offset_);


#if 0
    GstFormat fmt = GST_FORMAT_TIME;
    gint64 current = -1;

    Query the current position of the stream
    if (gst_element_query_position(pipeline_, &fmt, &current)) {
      RCLCPP_INFO_STREAM(get_logger(), "Position " << current);
    }
#endif

    // Stop on end of stream
    if (!buf) {
      RCLCPP_INFO(get_logger(), "Stream ended.");
      break;
    }

    // RCLCPP_DEBUG(get_logger(), "Got data.");

    // Get the image width and height
    GstPad * pad = gst_element_get_static_pad(sink_, "sink");
    const GstCaps * caps = gst_pad_get_current_caps(pad);
    GstStructure * structure = gst_caps_get_structure(caps, 0);
    gst_structure_get_int(structure, "width", &width_);
    gst_structure_get_int(structure, "height", &height_);

    // Update header information
    sensor_msgs::msg::CameraInfo cur_cinfo = camera_info_manager_.getCameraInfo();
    sensor_msgs::msg::CameraInfo::SharedPtr cinfo;
    cinfo.reset(new sensor_msgs::msg::CameraInfo(cur_cinfo));
    if (use_gst_timestamps_) {
      cinfo->header.stamp = rclcpp::Time(
        GST_TIME_AS_NSECONDS(buf->pts + bt) + time_offset_ - acquisition_offset_);
    } else {
      cinfo->header.stamp = now();
    }
    // RCLCPP_INFO(get_logger(), "Image time stamp: %.3f",cinfo->header.stamp.toSec());
    cinfo->header.frame_id = frame_id_;
    if (image_encoding_ == "jpeg") {
      sensor_msgs::msg::CompressedImage::SharedPtr img(new sensor_msgs::msg::CompressedImage());
      img->header = cinfo->header;
      img->format = "jpeg";
      img->data.resize(buf_size);
      std::copy(
        buf_data, (buf_data) + (buf_size),
        img->data.begin());
      jpeg_pub_->publish(*img);
      cinfo_pub_->publish(*cinfo);
    } else {
      // Complain if the returned buffer is smaller than we expect
      const unsigned int expected_frame_size =
        width_ * height_ * sensor_msgs::image_encodings::numChannels(image_encoding_);

      if (buf_size < expected_frame_size) {
        RCLCPP_WARN_STREAM(
          get_logger(), "GStreamer image buffer underflow: Expected frame to be " <<
            expected_frame_size << " bytes but got only " <<
            buf_size << " bytes. (make sure frames are correctly encoded)");
      }

      // Construct Image message
      sensor_msgs::msg::Image::SharedPtr img(new sensor_msgs::msg::Image());

      img->header = cinfo->header;

      // Image data and metadata
      img->width = width_;
      img->height = height_;
      img->encoding = image_encoding_;
      img->is_bigendian = false;
      img->data.resize(expected_frame_size);

      // Copy only the data we received
      // Since we're publishing shared pointers, we need to copy the image so
      // we can free the buffer allocated by gstreamer
      img->step = width_ * sensor_msgs::image_encodings::numChannels(image_encoding_);

      std::copy(
        buf_data,
        (buf_data) + (buf_size),
        img->data.begin());

      // Publish the image/info
      camera_pub_.publish(img, cinfo);
    }

    img_freq_diag_->tick(cinfo->header.stamp);

    // Release the buffer
    if (buf) {
      gst_memory_unmap(memory, &info);
      gst_memory_unref(memory);
      gst_sample_unref(sample);
    }
  }
}

void GSCam::cleanup_stream()
{
  RCLCPP_INFO(get_logger(), "Stopping gstreamer pipeline...");

#ifdef HAVE_GST_RTSP_SERVER
  if (rtsp_streaming_enabled_) {
    // Stop the RTSP server's GMainLoop first so no more media-configure callbacks fire.
    if (rtsp_main_loop_) {
      g_main_loop_quit(rtsp_main_loop_);
      rtsp_main_loop_thread_.join();
      g_main_loop_unref(rtsp_main_loop_);
      rtsp_main_loop_ = NULL;
    }
    if (rtsp_context_) {
      g_main_context_unref(rtsp_context_);
      rtsp_context_ = NULL;
    }
    // Release the appsrc reference obtained in on_rtsp_media_configure.
    {
      std::lock_guard<std::mutex> lock(rtsp_appsrc_mutex_);
      if (rtsp_appsrc_) {
        gst_object_unref(rtsp_appsrc_);
        rtsp_appsrc_ = NULL;
      }
    }
    if (rtsp_server_) {
      g_object_unref(rtsp_server_);
      rtsp_server_ = NULL;
    }
    // Release the extra ref from gst_bin_get_by_name.
    if (rtsp_appsink_) {
      gst_object_unref(rtsp_appsink_);
      rtsp_appsink_ = NULL;
    }
  }
#endif

  if (pipeline_) {
    gst_element_set_state(pipeline_, GST_STATE_NULL);
    // In tee mode sink_ carries an extra ref from gst_bin_get_by_name; release it
    // before the pipeline is destroyed so the refcount reaches zero cleanly.
    const bool need_tee = udp_streaming_enabled_
#ifdef HAVE_GST_RTSP_SERVER
      || rtsp_streaming_enabled_
#endif
    ;
    if (need_tee && sink_) {
      gst_object_unref(sink_);
      sink_ = NULL;
    }
    gst_object_unref(pipeline_);
    pipeline_ = NULL;
  }
}

void GSCam::run()
{
  if (!this->configure()) {
    RCLCPP_FATAL(get_logger(), "Failed to configure gscam!");
    return;
  }

  while (!stop_signal_ && rclcpp::ok()) {
    if (!this->init_stream()) {
      RCLCPP_FATAL(get_logger(), "Failed to initialize gscam stream!");
      break;
    }

    // Block while publishing
    this->publish_stream();

    this->cleanup_stream();

    RCLCPP_INFO(get_logger(), "GStreamer stream stopped!");

    if (reopen_on_eof_) {
      RCLCPP_INFO(get_logger(), "Reopening stream...");
    } else {
      RCLCPP_INFO(get_logger(), "Cleaning up stream and exiting...");
      break;
    }
  }
  rclcpp::shutdown();
}

// Example callbacks for appsink
// TODO(someone): enable callback-based capture
void gst_eos_cb(GstAppSink * appsink, gpointer user_data)
{
}
GstFlowReturn gst_new_preroll_cb(GstAppSink * appsink, gpointer user_data)
{
  return GST_FLOW_OK;
}
GstFlowReturn gst_new_asample_cb(GstAppSink * appsink, gpointer user_data)
{
  return GST_FLOW_OK;
}

// ---------------------------------------------------------------------------
// RTSP server support
// ---------------------------------------------------------------------------
#ifdef HAVE_GST_RTSP_SERVER

void GSCam::setup_rtsp_server()
{
  // A dedicated GMainContext + GMainLoop drives the RTSP server's network I/O
  // in a separate thread, isolated from GStreamer's own default context.
  rtsp_context_ = g_main_context_new();
  rtsp_main_loop_ = g_main_loop_new(rtsp_context_, FALSE);

  rtsp_server_ = gst_rtsp_server_new();
  gst_rtsp_server_set_service(rtsp_server_, std::to_string(rtsp_port_).c_str());

  // The factory pipeline is a single shared pipeline (one instance for all clients).
  // It sources from an appsrc named "pay_src" and packetizes via rtsp_pay_config_.
  // Caps on the appsrc must match what rtsp_encode_config_ produces; H264 byte-stream
  // is the default. Override rtsp_pay_config_ for other codecs.
  const std::string factory_desc =
    "( appsrc name=pay_src format=time is-live=true do-timestamp=false"
    "  caps=\"video/x-h264,stream-format=byte-stream,alignment=au\""
    "  ! " + rtsp_pay_config_ + " )";

  GstRTSPMediaFactory * factory = gst_rtsp_media_factory_new();
  gst_rtsp_media_factory_set_launch(factory, factory_desc.c_str());
  gst_rtsp_media_factory_set_shared(factory, TRUE);  // one pipeline, many viewers

  // When a client connects and the factory pipeline is instantiated, grab the appsrc.
  g_signal_connect(factory, "media-configure", G_CALLBACK(&GSCam::on_rtsp_media_configure), this);

  GstRTSPMountPoints * mounts = gst_rtsp_server_get_mount_points(rtsp_server_);
  gst_rtsp_mount_points_add_factory(mounts, rtsp_mount_point_.c_str(), factory);
  g_object_unref(mounts);

  gst_rtsp_server_attach(rtsp_server_, rtsp_context_);

  rtsp_main_loop_thread_ = std::thread(
    [this]()
    {
      g_main_context_push_thread_default(rtsp_context_);
      g_main_loop_run(rtsp_main_loop_);
      g_main_context_pop_thread_default(rtsp_context_);
    });

  RCLCPP_INFO_STREAM(
    get_logger(),
    "RTSP server listening on rtsp://0.0.0.0:" << rtsp_port_ << rtsp_mount_point_);
}

// Called by the RTSP server (on the GMainLoop thread) when a client connects and the
// shared factory pipeline is first created.  We retrieve the appsrc so that
// on_rtsp_new_sample can push encoded frames into it.
void GSCam::on_rtsp_media_configure(
  GstRTSPMediaFactory * /*factory*/, GstRTSPMedia * media, gpointer user_data)
{
  GSCam * self = static_cast<GSCam *>(user_data);

  GstElement * element = gst_rtsp_media_get_element(media);
  GstElement * appsrc = gst_bin_get_by_name_recurse_up(GST_BIN(element), "pay_src");
  gst_object_unref(element);

  std::lock_guard<std::mutex> lock(self->rtsp_appsrc_mutex_);
  if (self->rtsp_appsrc_) {
    gst_object_unref(self->rtsp_appsrc_);
  }
  self->rtsp_appsrc_ = appsrc;  // may be NULL if name not found

  if (self->rtsp_appsrc_) {
    RCLCPP_INFO(self->get_logger(), "RTSP client connected — appsrc ready, stream active.");
  } else {
    RCLCPP_ERROR(self->get_logger(), "RTSP media-configure: 'pay_src' appsrc not found.");
  }
}

// Called by the GStreamer appsink (on the pipeline thread) for every encoded frame.
// Pushes the buffer into the RTSP server's appsrc if a client is connected.
GstFlowReturn GSCam::on_rtsp_new_sample(GstAppSink * appsink, gpointer user_data)
{
  GSCam * self = static_cast<GSCam *>(user_data);

  GstSample * sample = gst_app_sink_pull_sample(appsink);
  if (!sample) {
    return GST_FLOW_OK;
  }

  {
    std::lock_guard<std::mutex> lock(self->rtsp_appsrc_mutex_);
    if (self->rtsp_appsrc_) {
      // Copy the buffer so the appsrc pipeline owns its own memory.
      GstBuffer * buf = gst_buffer_copy(gst_sample_get_buffer(sample));
      gst_app_src_push_buffer(GST_APP_SRC(self->rtsp_appsrc_), buf);
    }
  }

  gst_sample_unref(sample);
  return GST_FLOW_OK;
}

#endif  // HAVE_GST_RTSP_SERVER

}  // namespace gscam

#include "rclcpp_components/register_node_macro.hpp"

RCLCPP_COMPONENTS_REGISTER_NODE(gscam::GSCam)
