FROM vc-bds002.vimpelcom.global:5000/runner-hadoop:2.4.2
LABEL authors="Nikolay Skovpin <NSkovpin@vrn.beeline.ru>"
ARG target_file_name
ARG USER_INSTALL_NAME="tech_digital"
ARG USER_INSTALL_HOME="/home/${USER_INSTALL_NAME}"
RUN useradd -u 5000 -d ${USER_INSTALL_HOME} -s /bin/bash -m ${USER_INSTALL_NAME}
RUN env
ENV TARGET_JAR=${target_file_name}.jar
ENV PROJECT_NAME="DIM_DPC"
ENV PROJECT_DIR "$USER_INSTALL_HOME/Tasks"
RUN mkdir ${PROJECT_DIR}
#copy base project scripts
COPY oozie ${PROJECT_DIR}/oozie
#copy built jar
COPY target/${target_file_name}.jar ${PROJECT_DIR}/oozie/${PROJECT_NAME}/lib/${target_file_name}.jar
RUN ls -l ${PROJECT_DIR}/oozie/${PROJECT_NAME}/lib/${TARGET_JAR}
RUN chown -R $USER_INSTALL_NAME ${PROJECT_DIR}
RUN chmod -R 777 ${PROJECT_DIR}
USER ${USER_INSTALL_NAME}
WORKDIR ${USER_INSTALL_HOME}
ENV USER ${USER_INSTALL_NAME}
ENV HOME ${USER_INSTALL_HOME}
RUN ls -l ${PROJECT_DIR}/oozie/${PROJECT_NAME}
CMD kinit -kt $HOME/$USER.keytab $USER && ${PROJECT_DIR}/oozie/${PROJECT_NAME}/start_task.sh