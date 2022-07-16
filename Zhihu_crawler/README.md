*爬取知乎""热点问题""榜单, 不等价于热榜*

#### 使用方法

环境：见父文件夹中的conda环境（讲师所给的yaml配置文件即可）

设置：修改 zhihu.json

- 根据自己的信息修改mysql项，包括user, password, database项
- （可选）修改config项，`interval_between_board` 是两次爬取间隔秒数；`interval_between_question`是问题详细信息爬取间隔秒数

运行：进入配置好的conda环境后，输入python zhihu.py 即可运行。

得到的数据会存入对应数据库中；log信息也会输出到对应文件里。

