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

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

extern "C" {
#include "gst/gst.h"
#include "gst/app/gstappsink.h"
}

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
  // Follow a host clock step: re-base time_offset_ once two frames agree on a new offset
  void rebase_time_offset_on_clock_step();
  void cleanup_stream();

  void run();

  // General gstreamer configuration
  std::string gsconfig_;

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
  // Offset change treated as a host clock step rather than sampling jitter. Above NTP slew too:
  // at the 500 ppm slew limit the offset drifts 10 ms in 20 s, so a slewing clock re-bases at
  // most that often instead of every 2 s.
  static constexpr int64_t k_time_offset_step_ns = 10000000;
  // Candidate offset seen on the previous frame, awaiting confirmation
  std::optional<int64_t> pending_time_offset_;
  // Sim time drives now(), so the offset to the pipeline clock is not expected to be constant
  bool use_sim_time_ = false;
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
};

}  // namespace gscam

#endif  // GSCAM__GSCAM_HPP_
