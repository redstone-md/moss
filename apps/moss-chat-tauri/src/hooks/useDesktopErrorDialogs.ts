import { useEffect, useRef } from 'react'
import { showDesktopError } from '../lib/desktopDialogs'

type UseDesktopErrorDialogsOptions = {
  errors: string[]
}

export function useDesktopErrorDialogs({ errors }: UseDesktopErrorDialogsOptions) {
  const shownErrors = useRef<Set<string>>(new Set())

  useEffect(() => {
    for (const error of errors) {
      const normalized = error.trim()
      if (!normalized || shownErrors.current.has(normalized)) {
        continue
      }
      shownErrors.current.add(normalized)
      void showDesktopError('Moss Chat', normalized)
    }
  }, [errors])
}
