"""
配置使用 Edge 浏览器的脚本
适用于 Chrome 未安装或无法使用的情况（Windows）
"""

import os
import shutil

def backup_file(file_path):
    """备份文件"""
    backup_path = file_path + ".backup"
    if os.path.exists(file_path) and not os.path.exists(backup_path):
        shutil.copy2(file_path, backup_path)
        print(f"✓ 已备份: {backup_path}")

def modify_news_crawler():
    """修改新闻爬虫使用Edge"""
    file_path = "stock_data_news_collector/collectors/news_crawler.py"
    
    if not os.path.exists(file_path):
        print(f"✗ 文件不存在: {file_path}")
        return False
    
    backup_file(file_path)
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 替换导入
    content = content.replace(
        "from selenium import webdriver",
        "from selenium import webdriver\nfrom selenium.webdriver.edge.service import Service as EdgeService\nfrom webdriver_manager.microsoft import EdgeChromiumDriverManager"
    )
    
    # 替换Chrome初始化为Edge
    old_chrome_init = """            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)"""
    
    new_edge_init = """            # 使用 Edge 浏览器
            edge_service = EdgeService(EdgeChromiumDriverManager().install())
            edge_options = webdriver.EdgeOptions()
            
            # 复制所有Chrome选项到Edge
            for arg in chrome_options.arguments:
                edge_options.add_argument(arg)
            for key, value in chrome_options.experimental_options.items():
                edge_options.add_experimental_option(key, value)
            
            self.driver = webdriver.Edge(service=edge_service, options=edge_options)"""
    
    content = content.replace(old_chrome_init, new_edge_init)
    
    # 替换备用Chrome初始化
    old_chrome_fallback = """                self.driver = webdriver.Chrome(options=chrome_options)
                print("Browser started successfully (system ChromeDriver)")"""
    
    new_edge_fallback = """                # 尝试使用 Edge
                edge_options = webdriver.EdgeOptions()
                for arg in chrome_options.arguments:
                    edge_options.add_argument(arg)
                self.driver = webdriver.Edge(options=edge_options)
                print("Browser started successfully (system Edge)")"""
    
    content = content.replace(old_chrome_fallback, new_edge_fallback)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✓ 已修改: {file_path}")
    return True

def restore_backups():
    """恢复所有备份文件"""
    file_path = "stock_data_news_collector/collectors/news_crawler.py"
    backup_path = file_path + ".backup"
    
    if os.path.exists(backup_path):
        shutil.copy2(backup_path, file_path)
        print(f"✓ 已恢复: {file_path}")
        return True
    else:
        print("✗ 未找到备份文件")
        return False

if __name__ == "__main__":
    import sys
    
    print("=" * 60)
    print("Edge 浏览器配置工具")
    print("=" * 60)
    
    if len(sys.argv) > 1 and sys.argv[1] == "restore":
        print("\n恢复原始配置...")
        restore_backups()
        print("\n已恢复为使用 Chrome")
    else:
        print("\n配置使用 Edge 浏览器...")
        print("提示: 这将修改新闻爬虫代码使用Edge而不是Chrome")
        print()
        
        confirm = input("是否继续？(y/n): ").strip().lower()
        if confirm == 'y':
            if modify_news_crawler():
                print("\n" + "=" * 60)
                print("✓ 配置完成！")
                print("现在可以运行: python main_pipeline.py")
                print("\n如需恢复使用Chrome:")
                print("  python use_edge_browser.py restore")
                print("=" * 60)
        else:
            print("已取消")

