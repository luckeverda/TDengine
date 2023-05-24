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

#ifndef TDENGINE_OS_DIR_H
#define TDENGINE_OS_DIR_H

#ifdef __cplusplus
extern "C" {
#endif

void    taosRemoveDir(char *rootDir);
bool    taosDirExist(const char* dirname);
int32_t taosMkdirP(const char *pathname, int keepBase);
int32_t taosMkDir(const char *pathname, mode_t mode);
void    taosRemoveOldLogFiles(char *rootDir, int32_t keepDays);
int32_t taosRename(char *oldName, char *newName);
int32_t taosCompressFile(char *srcFileName, char *destFileName);

#if defined (_TD_SYLIXOS_)
  enum {
    WRDE_NOSPACE = 1, /* Ran out of memory.  */
    WRDE_BADCHAR,     /* A metachar appears in the wrong place.  */
    WRDE_BADVAL,      /* Undefined var reference with WRDE_UNDEF.  */
    WRDE_CMDSUB,      /* Command substitution with WRDE_NOCMD.  */
    WRDE_SYNTAX       /* Shell syntax error.  */
  };

  typedef struct {
    int we_wordc;
    char **we_wordv;
    int we_offs;
    char wordPos[20];
  } wordexp_t;

  typedef int(*__compar_fn_t)(const void *, const void *);

  int sendfile(int out_fd, int in_fd, off_t *offset, size_t size);

  #define llroundl llround
  #define openlog(...) {}
  #define syslog(...) {}
  #define closelog(...) {}
#endif

#ifdef __cplusplus
}
#endif

#endif
