import { request } from '@/utils/request'
export const AuthApi = {
  login: (credentials: { username: string; password: string }) => {
    // 直接使用明文用户名和密码，不再进行加密
    const entryCredentials = {
      username: credentials.username,
      password: credentials.password,
    }
    return request.post<{
      data: any
      token: string
    }>('/login/access-token', entryCredentials, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
    })
  },
  logout: () => request.post('/auth/logout'),
  info: () => request.get('/user/info'),
}
