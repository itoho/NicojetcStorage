# ワクワクストレージ

## 概要

ストレージ効率が良くて冗長性に優れた無料のストレージソフトウェア！

## よくあるエラー
### too many open filesと出る
あなたのSSDが早すぎたためにファイルの開ける数の制限に引っかかって起こります。

```sudo emacs /etc/sysctl.conf```

fs.inotify.max_user_instances = 2048

を追記してアローリ!(リロード)します

```
sudo sysctl -p
sudo sysctl -a
```