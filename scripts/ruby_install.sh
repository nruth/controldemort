cd
git clone git://github.com/sstephenson/rbenv.git .rbenv
echo 'export PATH="$HOME/.rbenv/bin:$PATH"' >> ~/.bash_profile
echo 'eval "$(rbenv init -)"' >> ~/.bash_profile

source ~/.bash_profile

mkdir -p ~/.rbenv/plugins
cd ~/.rbenv/plugins
git clone git://github.com/sstephenson/ruby-build.git

rbenv install 1.9.2-p320
rbenv global 1.9.2-p320
