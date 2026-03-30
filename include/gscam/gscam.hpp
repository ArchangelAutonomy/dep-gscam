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

#ifndef GSCAM__GSCAM_HPP_
#define GSCAM__GSCAM_HPP_

#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

extern "C" {
#include "gst/gst.h"
#include "gst/app/gstappsink.h"
#include "gst/app/gstappsrc.h"
}

#ifdef HAVE_GST_RTSP_SERVER
extern "C" {
#include "gst/rtsp-server/rtsp-server.h"
}
#endif

#include "rclcpp/rclcpp.hpp"

#include "image_transport/image_transport.hpp"
#include "camera_info_manager/camera_info_manager.hpp"

#include "sensor_msgs/msg/image.hpp"
#include "sensor_msgs/msg/compressed_image.hpp"
#include "sensor_msgs/msg/camera_info.hpp"
#include "sensor_msgs/srv/set_camera_info.hpp"
#include "std_msgs/msg/header.hpp"
#include "diagnostic_updater/diagnostic_updater.hpp"
#include "diagnostic_updater/publisher.hpp"

namespace gscam
{

class GSCam : public rclcpp::Node
{
public:
  explicit GSCam(const rclcpp::NodeOptions & options);
  ~GSCam();

private:
  bool configure();
  bool init_stream();
  void publish_stream();
  void cleanup_stream();

  void run();

#ifdef HAVE_GST_RTSP_SERVER
  void setup_rtsp_server();

  static void on_rtsp_media_configure(
    GstRTSPMediaFactory * factory, GstRTSPMedia * media, gpointer user_data);
  static GstFlowReturn on_rtsp_new_sample(GstAppSink * appsink, gpointer user_data);
#endif

  // General gstreamer configuration
  std::string gsconfig_;

  // UDP streaming configuration
  // When set, a tee splits the stream: one branch feeds the ROS appsink,
  // the other runs through udp_stream_config_ (e.g. hardware encoder + udpsink).
  bool udp_streaming_enabled_;
  std::string ros_branch_config_;   // pipeline from tee to appsink (ROS branch)
  std::string udp_stream_config_;   // pipeline from tee to udpsink (UDP branch)

  // Gstreamer structures
  GstElement * pipeline_;
  GstElement * sink_;

  // Appsink configuration
  bool sync_sink_;
  bool preroll_;
  bool reopen_on_eof_;
  bool use_gst_timestamps_;

  // Camera publisher configuration
  std::string frame_id_;
  int width_, height_;
  std::string image_encoding_;
  std::string camera_name_;
  std::string camera_info_url_;
  bool use_sensor_data_qos_;

  // ROS Inteface
  // Calibration between ros::Time and gst timestamps
  uint64_t time_offset_;
  // Calibration between gst_timestamps and camera shutter timestamps
  int64_t acquisition_offset_;
  camera_info_manager::CameraInfoManager camera_info_manager_;
  image_transport::CameraPublisher camera_pub_;
  // Case of a jpeg only publisher
  rclcpp::Publisher<sensor_msgs::msg::CompressedImage>::SharedPtr jpeg_pub_;
  rclcpp::Publisher<sensor_msgs::msg::CameraInfo>::SharedPtr cinfo_pub_;

  // Heartbeat
  rclcpp::Publisher<std_msgs::msg::Header>::SharedPtr heartbeat_pub_;
  rclcpp::TimerBase::SharedPtr heartbeat_timer_;

  // Diagnostics
  diagnostic_updater::Updater diag_updater_;
  double diag_min_freq_;
  double diag_max_freq_;
  std::unique_ptr<diagnostic_updater::TopicDiagnostic> img_freq_diag_;

  // Poll gstreamer on a separate thread
  std::thread pipeline_thread_;
  std::atomic<bool> stop_signal_;

#ifdef HAVE_GST_RTSP_SERVER
  // RTSP streaming
  // When rtsp_encode_config is non-empty, a tee branch encodes video into an appsink.
  // A GstRTSPServer bridges that appsink into an appsrc-based RTSP media factory so
  // multiple clients can connect to rtsp://<host>:<port><mount_point>.
  bool rtsp_streaming_enabled_;
  std::string rtsp_encode_config_;  // tee branch: encode pipeline before internal appsink
  std::string rtsp_pay_config_;     // RTP packetizer in the RTSP factory (e.g. rtph264pay)
  int rtsp_port_;
  std::string rtsp_mount_point_;

  GstElement * rtsp_appsink_;       // appsink in the main tee (capture encoded frames)
  GstElement * rtsp_appsrc_;        // appsrc inside the RTSP server pipeline (set on connect)
  std::mutex rtsp_appsrc_mutex_;

  GstRTSPServer * rtsp_server_;
  GMainContext * rtsp_context_;
  GMainLoop * rtsp_main_loop_;
  std::thread rtsp_main_loop_thread_;
#endif
};

}  // namespace gscam

#endif  // GSCAM__GSCAM_HPP_
