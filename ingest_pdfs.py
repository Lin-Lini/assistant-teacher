import argparse, os, sys, json, time, glob
from typing import Iterator, Tuple, List, Optional
import requests

# --- PDF extractors: pypdf primary, PyMuPDF fallback ---
def extract_with_pypdf(pdf_path: str) -> Iterator[Tuple[int, str]]:
    from pypdf import PdfReader
    from pypdf.errors import FileNotDecryptedError
    try:
        reader = PdfReader(pdf_path, strict=False)
        # Если зашифрован, пытаемся пустым паролем. Не вышло — выбрасываемся наружу.
        if getattr(reader, "is_encrypted", False):
            try:
                ok = reader.decrypt("")  # pypdf вернет 0/1/2
                if not ok:
                    raise FileNotDecryptedError("Encrypted and empty password failed")
            except Exception as e:
                raise FileNotDecryptedError(str(e))
        for i, page in enumerate(reader.pages, start=1):
            try:
                txt = page.extract_text() or ""
            except Exception:
                txt = ""
            yield i, txt.strip()
    except Exception as e:
        # Пробрасываем выше: пусть вызывающий решит, что делать
        raise e

def extract_with_pymupdf(pdf_path: str) -> Iterator[Tuple[int, str]]:
    import fitz  # PyMuPDF
    doc = fitz.open(pdf_path)
    try:
        if doc.is_encrypted:
            # Пустой пароль; если не взлетит — бросаем
            if not doc.authenticate(""):
                raise RuntimeError("Encrypted (PyMuPDF) and empty password failed")
        for i, page in enumerate(doc, start=1):
            try:
                txt = page.get_text("text") or ""
            except Exception:
                txt = ""
            yield i, txt.strip()
    finally:
        doc.close()

def extract_pages(pdf_path: str) -> Iterator[Tuple[int, str]]:
    # Сначала pypdf, если выстрелил — откатываемся на PyMuPDF
    try:
        yield from extract_with_pypdf(pdf_path)
        return
    except Exception as e:
        # Если это реально шифрование — не тратим время на fallback c тем же результатом
        msg = str(e).lower()
        if "decrypted" in msg or "encrypted" in msg or "password" in msg:
            raise  # пусть выше решают «скипнуть»
    # Пытаемся PyMuPDF
    try:
        yield from extract_with_pymupdf(pdf_path)
    except Exception as e:
        # Всё, сдаёмся
        raise e

def iter_pdf_files(root_dir: str, include_glob: Optional[str]) -> List[str]:
    pdfs = []
    if include_glob:
        # относительный паттерн из корня
        base = os.path.abspath(root_dir)
        for p in glob.glob(os.path.join(base, include_glob), recursive=True):
            if p.lower().endswith(".pdf"):
                pdfs.append(p)
    else:
        for dirpath, _, filenames in os.walk(root_dir):
            for fn in filenames:
                if fn.lower().endswith(".pdf"):
                    pdfs.append(os.path.join(dirpath, fn))
    return sorted(pdfs)

def batched(seq, size):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def embed_texts(vectorizer: str, texts: List[str]) -> Optional[List[List[float]]]:
    try:
        r = requests.post(
            f"{vectorizer.rstrip('/')}/embed",
            headers={"Content-Type": "application/json; charset=utf-8"},
            data=json.dumps({"texts": texts}),
            timeout=60,
        )
        if r.status_code != 200:
            print(f"[WARN] /embed {r.status_code}: {r.text[:200]}", flush=True)
            return None
        vecs = r.json().get("vectors")
        if not isinstance(vecs, list):
            print(f"[WARN] /embed no 'vectors' field", flush=True)
            return None
        return vecs
    except Exception as e:
        print(f"[WARN] /embed failed: {e}", flush=True)
        return None

def bulk_index(es: str, docs: List[dict], index: str, refresh: bool = False):
    lines = []
    for d in docs:
        lines.append(json.dumps({"index": {"_index": index}}, ensure_ascii=False))
        lines.append(json.dumps(d, ensure_ascii=False))
    data = "\n".join(lines) + "\n"
    params = {"refresh": "true"} if refresh else {}
    r = requests.post(f"{es.rstrip('/')}/_bulk", params=params,
                      headers={"Content-Type": "application/x-ndjson"}, data=data.encode("utf-8"), timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"Bulk error {r.status_code}: {r.text[:300]}")
    resp = r.json()
    if resp.get("errors"):
        # Печатаем первые 2 ошибки, чтобы не залить консоль
        bad = [it for it in resp.get("items", []) if it.get("index", {}).get("error")]
        print(f"[WARN] bulk had errors: {len(bad)}; sample: {bad[:2]}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf-dir", help="Папка с PDF (рекурсивно)")
    ap.add_argument("--pdf", help="Один файл PDF")
    ap.add_argument("--include-glob", help="Паттерн относительно --pdf-dir, например '**/Pdf/*.pdf'")
    ap.add_argument("--course-id", required=True)
    ap.add_argument("--es", default="http://localhost:9200")
    ap.add_argument("--vectorizer", default="http://localhost:8001")
    ap.add_argument("--index", default="chunks")
    ap.add_argument("--batch", type=int, default=32)
    ap.add_argument("--no-embeddings", action="store_true")
    args = ap.parse_args()

    if not args.pdf_dir and not args.pdf:
        print("Нужно --pdf-dir или --pdf", file=sys.stderr)
        return 2

    files = [args.pdf] if args.pdf else iter_pdf_files(args.pdf_dir, args.include_glob)
    if not files:
        print("[INFO] Нет PDF по указанному пути", flush=True)
        return 0

    total_docs = 0
    t0 = time.time()

    for fpath in files:
        relname = os.path.basename(fpath)
        try:
            pages = list(extract_pages(fpath))
        except Exception as e:
            emsg = str(e).lower()
            if "decrypt" in emsg or "encrypted" in emsg or "password" in emsg:
                print(f"[SKIP] Шифрованный PDF: {fpath} | {e}", flush=True)
            else:
                print(f"[SKIP] Не открылся PDF: {fpath} | {e}", flush=True)
            continue

        # фильтруем пустые страницы
        records = [{"course_id": args.course_id,
                    "filename": relname,
                    "page": pnum,
                    "content": text}
                   for (pnum, text) in pages if text]

        if not records:
            print(f"[INFO] Пусто в PDF: {fpath}", flush=True)
            continue

        for chunk in batched(records, args.batch):
            vectors = None
            if not args.no_embeddings:
                try:
                    vectors = embed_texts(args.vectorizer, [r["content"] for r in chunk])
                except Exception:
                    vectors = None

            docs = []
            if vectors and len(vectors) == len(chunk):
                for r, v in zip(chunk, vectors):
                    d = dict(r)
                    d["vector"] = v
                    docs.append(d)
            else:
                docs = chunk  # без векторов тоже норм

            bulk_index(args.es, docs, args.index, refresh=False)
            total_docs += len(docs)

        print(f"[OK] {relname}: страниц {len(pages)}, проиндексировано {len(records)}", flush=True)

    # финальный refresh
    try:
        requests.post(f"{args.es.rstrip('/')}/{args.index}/_refresh", timeout=30)
    except Exception:
        pass

    dt = time.time() - t0
    print(f"[DONE] Всего документов: {total_docs} за {dt:.1f}s", flush=True)
    return 0

if __name__ == "__main__":
    sys.exit(main())