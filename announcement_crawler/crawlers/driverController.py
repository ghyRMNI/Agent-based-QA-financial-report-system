from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import logging
import os
import time
import random


class DriverController:
    """
    WebDriver controller for browser automation and file downloads
    """

    def __init__(
        self,
        driver: webdriver.Edge = None,
        download_dir: str = None,
        logger: logging.Logger = None,
    ):
        self.driver = driver
        self.logger = logger or self._setup_default_logger()
        self.download_dir = (
            download_dir or "outputs/announcements"
        )  # default settings
        os.makedirs(self.download_dir, exist_ok=True)
        self._is_self_managed_driver = False

    def _setup_default_logger(self) -> logging.Logger:
        """
        - Create default logger for DriverController
        - Input: None
        - Output: Configured logger instance
        """
        logger = logging.getLogger("DriverController")
        # logger.setLevel(logging.INFO)
        logger.setLevel(logging.WARNING)  # Set to WARNING to reduce log output
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _setup_driver_options(
        self, download_dir: str, headless: bool = False
    ) -> webdriver.EdgeOptions:
        """
        - Set up Edge options for browser automation
        - Input:
            - download_dir: Directory to store downloaded files
            - headless: Whether to run browser in headless mode
        - Output: Configured EdgeOptions instance
        """
        options = webdriver.EdgeOptions()
        if headless:
            options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-blink-features=AutomationControlled")
        
        # 禁用浏览器日志输出
        options.add_argument("--log-level=3")  # 只显示致命错误
        options.add_argument("--silent")
        options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
        
        prefs = {
            "download.default_directory": os.path.abspath(download_dir),
            "download.prompt_for_download": False,
            "plugins.always_open_pdf_externally": True,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
        }
        options.add_experimental_option("prefs", prefs)
        return options

    def start_browser(self, headless: bool = False) -> None:
        """
        - Start Chrome browser with configured options
        - Input:
            - headless: Whether to run browser in headless mode
            - download_dir: Directory to store downloaded files
        - Output: None
        """
        download_dir = self.download_dir
        if self.driver is not None:
            self.logger.warning("Browser already initialized")
            return
        options = self._setup_driver_options(
            download_dir=download_dir, headless=headless
        )
        try:
            # load edge driver
            from selenium.webdriver.edge.service import Service
            import subprocess

            driver_path = r"D:\edgeDriver\msedgedriver.exe"
            # 禁用EdgeDriver日志输出
            service = Service(
                executable_path=driver_path,
                log_output=subprocess.DEVNULL
            )
            self.driver = webdriver.Edge(service=service, options=options)

            self.driver.maximize_window()
            self._is_self_managed_driver = True
            self.logger.info(
                f"Browser started with download path: {os.path.abspath(download_dir)}"
            )
        except Exception as e:
            self.logger.error(f"Failed to start browser: {str(e)}")
            raise

    def _wait_and_highlight(
        self, by: str, locator: str, timeout: int = 10, highlight_color: str = "red"
    ):
        """
        - 等待并高亮元素
        - 输入：
        - by: Locator strategy (e.g., By.ID, By.CSS_SELECTOR)
        - locator: Element locator expression
        - timeout: Maximum wait time in seconds
        - highlight_color: Color to use for highlighting
        - Output: Found web element
        """
        context = self.driver
        element = WebDriverWait(context, timeout).until(
            EC.presence_of_element_located((by, locator))
        )
        self.driver.execute_script(
            f"arguments[0].style.border='3px solid {highlight_color}';", element
        )
        time.sleep(random.uniform(0.5, 1.0))
        return element

    def _reliable_click(self, element):
        """
        - Reliably click an element, handling common click issues
        - Input:
            - element: Web element to click
        - Output: None
        """
        try:
            element.click()
        except:
            try:
                ActionChains(self.driver).move_to_element(element).pause(
                    random.uniform(0.5, 1.0)
                ).click().perform()
            except:
                self.driver.execute_script("arguments[0].click();", element)

    def _take_screenshot(self, prefix="error"):
        """
        - Take a screenshot of the current page
        - Input:
            - prefix: Prefix for screenshot filename
        - Output: None (saves screenshot file)
        """
        if not self.driver:
            return ""

        screenshot_dir = "logger/error_screenshots"
        try:
            os.makedirs(screenshot_dir, exist_ok=True)
            filename = f"{screenshot_dir}/{prefix}.png"
            self.driver.save_screenshot(filename)
            self.logger.info(f"Screenshot saved: {filename}")
            return filename
        except Exception as e:
            self.logger.error(f"Failed to save screenshot: {str(e)}")
            # 如果保存失败，尝试清理可能创建的空目录
            self._cleanup_empty_dir(screenshot_dir)
            return ""
    
    def _cleanup_empty_dir(self, dir_path: str):
        """
        清理空目录
        
        Args:
            dir_path: 目录路径
        """
        try:
            if os.path.exists(dir_path) and os.path.isdir(dir_path):
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
                    # 如果父目录也是空的，也删除
                    parent_dir = os.path.dirname(dir_path)
                    if parent_dir and os.path.exists(parent_dir) and os.path.isdir(parent_dir):
                        if not os.listdir(parent_dir):
                            os.rmdir(parent_dir)
        except OSError:
            # 忽略删除失败的情况
            pass

    def close(self):
        """
        - Close browser and clean up resources
        - Input: None
        - Output: None
        """
        if self.driver and self._is_self_managed_driver:
            self.driver.quit()
            self.logger.info("Browser closed")
        self.driver = None
