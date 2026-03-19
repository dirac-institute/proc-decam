FROM lsstsqre/centos:7-stack-lsst_distrib-w_2024_34
USER root
# install packages
RUN yum -y install curl ca-certificates && yum clean all && \
    update-ca-trust force-enable || true && \
    curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.rpm.sh | bash && \
    yum -y install git-lfs && \
    git lfs install --system
RUN yum -y install postgresql-server postgresql-contrib && yum clean all

USER lsst
WORKDIR /home/lsst
RUN source /opt/lsst/software/stack/loadLSST.bash && \
    setup lsst_distrib && \
    butler create ./repo && \
    butler register-instrument ./repo lsst.obs.decam.DarkEnergyCamera && \
    butler write-curated-calibrations ./repo lsst.obs.decam.DarkEnergyCamera && \
    butler register-skymap ./repo -c name='discrete'
