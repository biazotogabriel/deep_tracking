from setuptools import setup

setup(
    name='deep_tracking',
    version='1.0.4',
    description='Library to track the development of data science works using the DE&P method',
    long_description = 'Library to track the development of data science works using the DE&P method',
    author='Gabriel Nuernberg Biazoto',
    author_email='biazotogabriel@gmail.com',
    url='https://github.com/biazotogabriel/deep_tracking',
    packages=['deep_tracking'],
    license = 'MIT',
    keywords = 'deep de&p data science',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    python_requires='>=3.6',
    install_requires=[
        'pandas==1.4.4'
    ],
)
