# digester

### Packages
pip install puka
pip install MySQL-python

### Starting the service
```
cd /opt/scripts/
git clone https://github.com/BlackMesh/digester.git
ln -s /opt/scripts/digester/init.d/digester /etc/init.d/
cp digester.conf.example /etc/digester.conf
chkconfig digester on
service digester start
```
