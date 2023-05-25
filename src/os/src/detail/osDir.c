/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "os.h"
#include "tglobal.h"
#include "tulog.h"
#include "zlib.h"

#define COMPRESS_STEP_SIZE 163840

void taosRemoveDir(char *rootDir) {
  DIR *dir = opendir(rootDir);
  if (dir == NULL) return;

  struct dirent *de = NULL;
  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

    char filename[1024];
    snprintf(filename, 1023, "%s/%s", rootDir, de->d_name);
    if (de->d_type & DT_DIR) {
      taosRemoveDir(filename);
    } else {
      (void)remove(filename);
      uInfo("file:%s is removed", filename);
    }
  }

  closedir(dir);
  rmdir(rootDir);

  uInfo("dir:%s is removed", rootDir);
}

bool taosDirExist(const char* dir) {
  return access(dir, F_OK) == 0;
}

int32_t taosMkdirP(const char *dir, int keepLast) {
  char tmp[256];
  char *p = NULL;
  size_t len;
  size_t i;

  snprintf(tmp, sizeof(tmp),"%s",dir);
  len = strlen(tmp);
  if (!keepLast) {
    for (i = len - 1; i > 0; --i)
      if (tmp[i] == '/') {
	tmp[i] = 0;
	break;
      }
  }

  for (p = tmp + 1; *p; p++)
      if (*p == '/') {
          *p = 0;
          if (mkdir(tmp, S_IRWXU) && errno != EEXIST)
            return -1;
          *p = '/';
      }
  if (mkdir(tmp, S_IRWXU) && errno != EEXIST)
    return -1;

  return 0;
}

int32_t taosMkDir(const char *path, mode_t mode) {
  int code = mkdir(path, mode);
  if (code < 0 && errno == EEXIST) code = 0;
  return code;
}

void taosRemoveOldLogFiles(char *rootDir, int32_t keepDays) {
  DIR *dir = opendir(rootDir);
  if (dir == NULL) return;

  int64_t        sec = taosGetTimestampSec();
  struct dirent *de = NULL;

  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

    char filename[1024];
    snprintf(filename, 1023, "%s/%s", rootDir, de->d_name);
    if (de->d_type & DT_DIR) {
      continue;
    } else {
      int32_t len = (int32_t)strlen(filename);
      if (len > 3 && strcmp(filename + len - 3, ".gz") == 0) {
        len -= 3;
      }

      int64_t fileSec = 0;
      for (int i = len - 1; i >= 0; i--) {
        if (filename[i] == '.') {
          fileSec = atoll(filename + i + 1);
          break;
        }
      }

      if (fileSec <= 100) continue;
      int32_t days = (int32_t)(ABS(sec - fileSec) / 86400 + 1);
      if (days > keepDays) {
        (void)remove(filename);
        uInfo("file:%s is removed, days:%d keepDays:%d", filename, days, keepDays);
      } else {
        uTrace("file:%s won't be removed, days:%d keepDays:%d", filename, days, keepDays);
      }
    }
  }

  closedir(dir);
  rmdir(rootDir);
}

int32_t taosCompressFile(char *srcFileName, char *destFileName) {
  int32_t ret = 0;
  int32_t len = 0;
  char *  data = malloc(COMPRESS_STEP_SIZE);
  FILE *  srcFp = NULL;
  gzFile  dstFp = NULL;

  srcFp = fopen(srcFileName, "r");
  if (srcFp == NULL) {
    ret = -1;
    goto cmp_end;
  }

  int32_t fd = open(destFileName, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd < 0) {
    ret = -2;
    goto cmp_end;
  }

  dstFp = gzdopen(fd, "wb6f");
  if (dstFp == NULL) {
    ret = -3;
    close(fd);
    goto cmp_end;
  }

  while (!feof(srcFp)) {
    len = (int32_t)fread(data, 1, COMPRESS_STEP_SIZE, srcFp);
    (void)gzwrite(dstFp, data, len);
  }

cmp_end:
  if (srcFp) {
    fclose(srcFp);
  }
  if (dstFp) {
    gzclose(dstFp);
  }
  free(data);

  return ret;
}


#ifdef _TD_SYLIXOS_
#define MIN_CHUNK 64

int
getstr(lineptr, n, stream, terminator, offset)
char **lineptr;
size_t *n;
FILE *stream;
char terminator;
int offset;
{
    int nchars_avail;       /* Allocated but unused chars in *LINEPTR.  */
    char *read_pos;     /* Where we're reading into *LINEPTR. */
    int ret;

    if (!lineptr || !n || !stream)
    {
        errno = EINVAL;
        return -1;
    }

    if (!*lineptr)
    {
        *n = MIN_CHUNK;
        *lineptr = malloc(*n);
        if (!*lineptr)
        {
            errno = ENOMEM;
            return -1;
        }
    }

    nchars_avail = (int)(*n - offset);
    read_pos = *lineptr + offset;

    for (;;)
    {
        int save_errno;
        register int c = getc(stream);

        save_errno = errno;

        /* We always want at least one char left in the buffer, since we
        always (unless we get an error while reading the first char)
        NUL-terminate the line buffer.  */

        assert((*lineptr + *n) == (read_pos + nchars_avail));
        if (nchars_avail < 2)
        {
            if (*n > MIN_CHUNK)
                *n *= 2;
            else
                *n += MIN_CHUNK;

            nchars_avail = (int)(*n + *lineptr - read_pos);
            *lineptr = realloc(*lineptr, *n);
            if (!*lineptr)
            {
                errno = ENOMEM;
                return -1;
            }
            read_pos = *n - nchars_avail + *lineptr;
            assert((*lineptr + *n) == (read_pos + nchars_avail));
        }

        if (ferror(stream))
        {
            /* Might like to return partial line, but there is no
            place for us to store errno.  And we don't want to just
            lose errno.  */
            errno = save_errno;
            return -1;
        }

        if (c == EOF)
        {
            /* Return partial line, if any.  */
            if (read_pos == *lineptr)
                return -1;
            else
                break;
        }

        *read_pos++ = c;
        nchars_avail--;

        if (c == terminator)
            /* Return the line.  */
            break;
    }

    /* Done - NUL terminate and return the number of chars read.  */
    *read_pos = '\0';

    ret = (int)(read_pos - (*lineptr + offset));
    return ret;
}

int
getline(lineptr, n, stream)
char **lineptr;
size_t *n;
FILE *stream;
{
    return getstr(lineptr, n, stream, '\n', 0);
}
#endif


#ifdef _TD_SYLIXOS_

void wordfree(wordexp_t *pwordexp) {}

int wordexp(const char *words, wordexp_t *pwordexp, int flags) {
  pwordexp->we_offs = 0;
  pwordexp->we_wordc = 1;
  pwordexp->we_wordv = (char **)(pwordexp->wordPos);
  pwordexp->we_wordv[0] = (char *)words;
  return 0;
}

int sendfile(int out_fd, int in_fd, off_t *offset, size_t size)
{
    char buf[8192];
    int readn = size > sizeof(buf) ? sizeof(buf) : size;

    int ret;
    //带偏移量地原子的从文件中读取数据
    int n = pread(in_fd, buf, readn, *offset);

    if (n > 0)
    {
        ret = write(out_fd, buf, n);
        if (ret < 0)
        {
            perror("write() failed.");
        }
        else
        {
            *offset += ret;
        }
        return ret;
    }
    else
    {
        perror("pread() failed.");
        return -1;
    }
}

#define IS_SLASH_P(c) (*(c) == '/' || *(c) == '\\')
// 对于合法的路径输入，返回其上一级目录，不包含目录分隔符
// 返回值为pcDir的长度

int taosDirName(const char *path, char *pcDir) {
    int         len = 0;
    const char *endp = NULL;
    const char *runp = NULL;

    *pcDir = '\0';
    if (path == NULL || *path == '\0') {
        return len;
    }

    runp = path + strlen(path) - 1;
    while (runp > path && IS_SLASH_P(runp)) --runp;
    if (runp == path) {
        endp = path;
    } else {
        while (runp > path && !IS_SLASH_P(runp)) --runp;
        if (runp == path) {
            endp = path;
        } else {
            while (runp > path && IS_SLASH_P(runp)) --runp;
            endp = runp;
        }
    }

    if (endp != path) {
        len = endp - path + 1;
        memcpy(pcDir, path, len);
        pcDir[len] = '\0';
    } else {
        if (*path == '/')  // for Linux
        {
            strcpy(pcDir, "/");
            len = 1;
        } else if (path[1] == ':')  // for Windows
        {
            memcpy(pcDir, path, 2);
            pcDir[2] = '\0';
            len = 2;
        } else {
            strcpy(pcDir, ".");
            len = 1;
        }
    }
    return len;
}

#else
  
int taosDirName(const char *path, char *pcDir) {
  char *p = strdup(dirname(path));
  strncpy(pcDir, p, PATH_MAX);
  free(p);
  return strlen(pcDir);
}

#endif