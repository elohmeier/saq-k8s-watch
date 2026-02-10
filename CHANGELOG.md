# CHANGELOG

<!-- version list -->

## v1.1.0 (2026-02-10)

### Features

- Support custom queue class, improve logging and error handling
  ([`6843211`](https://github.com/elohmeier/saq-k8s-watch/commit/6843211c98af17bc059bdec4947315a3d5887fef))

### Refactoring

- Convert entrypoint to pyproject script with structured logging
  ([`213d386`](https://github.com/elohmeier/saq-k8s-watch/commit/213d38645b63b74bad9329230b5c50d48097a271))


## v1.0.4 (2026-02-09)

### Bug Fixes

- Pin psycopg-pool <3.3.0 for saq compatibility
  ([`1679652`](https://github.com/elohmeier/saq-k8s-watch/commit/1679652396d585a06d5d84de9ba5aca21cb63fc1))


## v1.0.3 (2026-02-09)

### Bug Fixes

- Add psycopg-binary to avoid libpq system dependency
  ([`a2d4633`](https://github.com/elohmeier/saq-k8s-watch/commit/a2d4633ad451048de16bacaa8cb6cc62191d43df))


## v1.0.2 (2026-02-09)

### Bug Fixes

- Add saq postgres extra for psycopg dependency
  ([`d371d3e`](https://github.com/elohmeier/saq-k8s-watch/commit/d371d3e4b3c69e27dc576f6c60aa6db4bced15e1))

- Cancel in-progress CI/CD runs on newer pushes
  ([`41163a2`](https://github.com/elohmeier/saq-k8s-watch/commit/41163a245e5ca6d965fe873e291110b80223063c))

- Use native ARM64 runners for Docker builds
  ([`7b15f81`](https://github.com/elohmeier/saq-k8s-watch/commit/7b15f81847a9669ba0d7a9e5cd47ba2941fc3496))


## v1.0.1 (2026-02-09)

### Bug Fixes

- Remove unused imports in test file
  ([`53606b3`](https://github.com/elohmeier/saq-k8s-watch/commit/53606b3747e1f166f5f10afc7c07ba4f0b374c22))


## v1.0.0 (2026-02-09)

- Initial Release
