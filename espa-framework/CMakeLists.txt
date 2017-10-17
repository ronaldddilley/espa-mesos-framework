cmake_minimum_required ( VERSION 2.8 )


set ( IMAGE_NAME ${SYSTEM_NAME}/framework )


add_custom_target (
    framework-image
    COMMAND docker build -t ${IMAGE_NAME} -f Dockerfile .
    COMMAND docker tag ${IMAGE_NAME} ${IMAGE_NAME}:${SYSTEM_VERSION}
)
