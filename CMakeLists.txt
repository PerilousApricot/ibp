cmake_minimum_required(VERSION 2.6)

# make a project
project( IBP CXX )

# get dependancies
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
find_package(BerkeleyDB REQUIRED)
find_package(GLIB2 REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Protobufc REQUIRED)
include_directories(${BDB_INCLUDE_DIR} ${GLIB2_INCLUDE_DIRS}
${OPENSSL_INCLUDE_DIR} "ceph/eobfs" "ceph" ${PROTOBUF_INCLUDE_DIR})
SET(LIBS ${LIBS} ${BDB_LIBRARIES} ${GLIB2_LIBRARIES} ${OPENSSL_LIBRARIES}
${CRYPTO_LIBRARIES} ${PROTOBUF_LIBRARY}
pthread)

# config options
OPTION( _ENABLE_PHOEBUS "Enable Phoebus support" OFF )
CONFIGURE_FILE( ${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_SOURCE_DIR}/config.h )

# common objects
SET(RESOURCE_OBJS resource alloc_history cap_timestamp random osd_fs.cc osd_ebofs.cc db_resource log debug ceph/config.cc ceph/common/Clock.cc )
SET(NETWORK_OBJS network net_sock net_1_ssl net_2_ssl net_fd net_phoebus phoebus )

# /protobuf stuff
WRAP_PROTO(PROTO_SRC "alog.proto")
#ADD_EXECUTABLE(server ${server_SRC} ${PROTO_SRC})

message("proto src is ${PROTO_SRC}")
# various executables
ADD_EXECUTABLE( ibp_server server_lib ibp_server  global_data
string_token parse_commands install_commands commands
handle_commands resource_list subnet security_log stack
task_coordinator buffer_transfer dns_cache transfer_stats
server_version lock_alloc activity_log thread_slots pigeon_hole envelope
envelope_net ${RESOURCE_OBJS} ${NETWORK_OBJS} ${PROTO_SRC})

ADD_EXECUTABLE( upgrade_v120 ${RESOURCE_OBJS}  ${NETWORK_OBJS} string_token
dns_cache global_data  upgrade_resource_v1-2-0
ibp_upgrade_v1-2-0)
ADD_EXECUTABLE(mkfs  mkfs_resource ${RESOURCE_OBJS})
ADD_EXECUTABLE(fmtrid fmtrid ${RESOURCE_OBJS})
#ADD_EXECUTABLE(test_res test_resource ${RESOURCE_OBJS})
#ADD_EXECUTABLE(rperf rperf ${RESOURCE_OBJS}) 
#ADD_EXECUTABLE(rand random)
ADD_EXECUTABLE(urand64 urand64 random)
ADD_EXECUTABLE(get_version get_version log dns_cache string_token ${NETWORK_OBJS})
ADD_EXECUTABLE(get_alloc get_alloc log dns_cache string_token subnet ${NETWORK_OBJS})
ADD_EXECUTABLE(date_spacefree date_spacefree log dns_cache string_token ${NETWORK_OBJS})
ADD_EXECUTABLE(expire_list expire_list log dns_cache string_token ${NETWORK_OBJS})
ADD_EXECUTABLE(print_alog activity_log print_alog log subnet string_token
dns_cache stack ${PROTO_SRC} ${NETWORK_OBJS} envelope envelope_net)

SET_TARGET_PROPERTIES(ibp_server PROPERTIES LINKER_LANGUAGE CXX)
TARGET_LINK_LIBRARIES( ibp_server ${LIBS})
TARGET_LINK_LIBRARIES( upgrade_v120 ${LIBS})
TARGET_LINK_LIBRARIES( mkfs ${LIBS})
TARGET_LINK_LIBRARIES( fmtrid ${LIBS})
#TARGET_LINK_LIBRARIES( test_res ${LIBS})
#TARGET_LINK_LIBRARIES( rperf ${LIBS})
#TARGET_LINK_LIBRARIES( rand ${LIBS})
TARGET_LINK_LIBRARIES( urand64 ${LIBS})
TARGET_LINK_LIBRARIES( get_version ${LIBS})
TARGET_LINK_LIBRARIES( get_alloc ${LIBS})
TARGET_LINK_LIBRARIES( date_spacefree ${LIBS})
TARGET_LINK_LIBRARIES( expire_list ${LIBS})
TARGET_LINK_LIBRARIES( print_alog ${LIBS})

