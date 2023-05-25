import setuptools

with open("README", 'r') as f:
    long_description = f.read()

REQUIREMENTS = [i.strip() for i in open("requirements.txt").readlines()]

setuptools.setup(
    name='FetchDCCResult',
    version='1.0',
    description='A Python client to fetch feedback of submitted data, file and images to DCC',
    author='Tianyu Chen',
    author_email='tianyu.chen@jax.org',
    url="https://github.com/TheJacksonLaboratory/FetchDCCResult",
    packages=setuptools.find_packages(),
    install_requires=REQUIREMENTS,
    python_requires='>=3.8',
    scripts=[
        "scripts/impc.sh",
        "scripts/ebi.sh",
    ]
)
