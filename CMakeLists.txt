cmake_minimum_required ( VERSION 2.8 )


include ( InstallExecutable.cmake )


#set ( PROJECT_INSTALL_PATH ${CMAKE_INSTALL_PREFIX}/usgs/espa )
#set ( LINK_PROJECT_EXECUTABLES true )


project ( espa )


set ( SYSTEM_NAME espa )
set ( SYSTEM_VERSION 10.0.0 )


add_subdirectory ( espa-framework )
