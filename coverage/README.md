# Vdsm coverage

This package contains support files for creating code coverage report.

## Files

- /etc/vdsm/coverage.conf - coverage configuration file
- /etc/sysconfig/vdsm - environment file for enabling coverage

## Recording code coverage

1.  Enable coverage in vdsm.conf:

        [devel]
        coverage_enable = true

2.  Retart vdsm
3.  Run functional tests, or perform some flows manually
4.  Stop vdsm

Coverage data file is stored in /var/run/vdsm/vdsm.coverage.

## Creating html report

Run this to create html report in the htmlcov directory:

    coverage html --rcfile /etc/vdsm/coverage.conf

To view the report, open a web server in htmlcov directory:

    cd htmlcov
    python -m SimpleHTTPServer

The report is available in your favorite browser at:

    http://myhost.example.com:8000/

## Notes

On EL7, coverage is too old. Use pip to install a recent version:

    pip install -U coverage

Note that coverage data files generated with coverage 3.6 cannot be processed
by coverage 4.
