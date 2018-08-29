# spider
单个spider，master分支充当sample project的作用
用户需要创建对应网站的分支

爬虫开发工作流程:
文档学习
   superbase/doc
   spiderx/doc


本地工作
  1.0 选取一个工作目录workdir
  1.1 git clone superSpider/spider.git
      git clone superSpider/spiderx.git
      git clone superSpider/superbase.git
  1.2 cd spider
      git checkout -b {website}
      website用网站全名，如company_51job_com,bbs_csdn_net,www_zhihu_com
      在这个{website}分支上工作，提交维护这个分支
  1.3 mkdir {website}
      在这个目录上工作


最终项目结构：
    workdir
        spider
            {website}
                #这下面的子结构爬虫工程师自己定义
        spiderx
        superbase

4, 参考sample，开始工作，测试,

5, 提交{website}分支



