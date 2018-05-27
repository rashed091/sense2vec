# Training sense2vec model setup
- #### **Make sure that your home directory name is newscred**

  - #### If not then need to make some local changes to run the code correctly

- #### **git clone git@github.com:rashed091/sense2vec.git in home directory**

- #### **install all the requirements file in system python**

  - #### **bash install.sh**

- #### **move .service and .timer file in /etc/systemd/system**

- #### **systemctl enable space-file-processing.service**

- #### **systemctl enable space-file-processing.timer**

- #### check systemctl status space-file-processing.service

- #### **Done!**

# How To Stop the daemon and kill the running process

- **systemctl stop/disable space-file-processing.service**

- **systemctl stop/disable space-file-processing.timer**

- **sudo kill -9 process_id**

  

