# 비밀번호 자동 완성 확인

이 절차는 폼의 브라우저 호환성을 확인한다. 자동 완성 제안은 브라우저, 저장된 자격 증명, 사이트 출처와 배포 보안 설정에 따라 달라진다. 신뢰된 HTTPS 출처에서 확인한다. plain HTTP에서 제안이 보이지 않는 현상만으로 폼 오류라고 판단하지 않는다.

## iOS Safari

### 기존 계정

1. Safari에서 `/login`을 연다.
2. 계정 이름 또는 비밀번호 입력란을 누르고 저장된 사이트 자격 증명이 제안되는지 확인한다.
3. 자격 증명을 선택해 계정 이름과 비밀번호가 모두 채워지는지 확인한다.
4. 로그인하고 인증에 성공하는지 확인한다.

### 새 설치

1. 관리자 설정을 열고 계정이 `owner`로 표시되는지 확인한다.
2. Safari가 제안하는 암호를 사용할 수 있으면 선택하고 비밀번호 확인란에도 입력한다.
3. 설정을 완료한 뒤 로그아웃하고 `/login`을 연다.
4. 저장한 `owner` 자격 증명이 제안되고 로그인되는지 확인한다.

### 설정 이어하기

1. 아직 설정이 끝나지 않은 설치의 이어하기 화면을 연다.
2. 계정 `owner`와 현재 비밀번호 입력란에 저장된 자격 증명이 제안되는지 확인한다.
3. 로그인해 설정이 이어지는지 확인한다.

사이트는 localhost, IP 주소, Tailscale 이름, 역방향 프록시 도메인마다 서로 다른 출처로 취급될 수 있다. 암호 관리자는 출처별로 자격 증명을 저장하므로 애플리케이션이 서로 다른 출처의 자격 증명을 합치지 않는다.

## English

This procedure checks the browser form contract. Suggestions depend on the browser, saved credentials, site origin, and deployment security settings. Verify on a trusted HTTPS origin; a missing suggestion on plain HTTP alone does not indicate a form defect.

### Existing account on iOS Safari

1. Open `/login` in Safari.
2. Focus the username or password field and check whether the saved site credential is offered.
3. Select it and confirm both username and password are filled.
4. Submit and confirm sign-in succeeds.

### New installation

1. Open administrator setup and confirm the account is shown as `owner`.
2. If Safari offers a generated password, use it and fill the confirmation field too.
3. Complete setup, sign out, and open `/login`.
4. Confirm the saved `owner` credential is offered and sign-in succeeds.

### Resume setup

1. Open the resume screen for an installation whose setup is incomplete.
2. Confirm Safari offers the saved credential for account `owner` and the current-password field.
3. Sign in and confirm setup resumes.

The browser may treat localhost, an IP address, a Tailscale name, and a reverse-proxy domain as separate origins. Password managers save credentials per origin; the application does not merge credentials across origins.
